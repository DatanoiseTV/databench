//! memcached benchmark — minimal async text-protocol client. Sandboxed by
//! default with a unique key prefix and a short TTL on every SET so that
//! even hard-killed runs don't leave keys behind. Defaults follow
//! `memtier_benchmark`: 1:10 SET:GET on 32-byte values.
//!
//! We don't depend on any memcached crate — the text protocol fits in
//! ~150 lines of `tokio::io` and avoids pulling in another dependency.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use crate::stats::WorkerReport;
use crate::worker::{try_reserve_budget, WorkerHandles};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum McWorkload {
    Read,
    /// 1:10 SET:GET, 32-byte values — memtier_benchmark canon.
    Memtier,
    Mixed,
    Write,
}

impl McWorkload {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "read" | "get" | "get-only" => Ok(Self::Read),
            "memtier" => Ok(Self::Memtier),
            "mixed" | "mix" => Ok(Self::Mixed),
            "write" | "set" | "set-only" => Ok(Self::Write),
            other => Err(anyhow!(
                "unknown workload '{other}'; expected read | memtier | mixed | write"
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct McConfig {
    pub host: String,
    pub port: u16,
    pub prefix: String,
    pub workload: McWorkload,
    pub seed_keys: usize,
    pub seed_value_size: usize,
    pub seed_ttl: u32,
    pub timeout: Duration,
    pub custom: Option<String>,
}

pub struct McClient {
    stream: BufStream<TcpStream>,
    line: String,
}

impl McClient {
    pub async fn connect(host: &str, port: u16) -> Result<Self> {
        let stream = TcpStream::connect((host, port))
            .await
            .with_context(|| format!("connect {host}:{port}"))?;
        let _ = stream.set_nodelay(true);
        Ok(Self {
            stream: BufStream::new(stream),
            line: String::with_capacity(256),
        })
    }

    pub async fn version(&mut self) -> Result<String> {
        self.stream.write_all(b"version\r\n").await?;
        self.stream.flush().await?;
        self.line.clear();
        self.stream.read_line(&mut self.line).await?;
        let v = self
            .line
            .strip_prefix("VERSION ")
            .ok_or_else(|| anyhow!("unexpected: {}", self.line.trim_end()))?
            .trim_end()
            .to_string();
        Ok(v)
    }

    pub async fn get(&mut self, key: &str) -> Result<Option<u64>> {
        // Returns the byte length of the value, or None if missing.
        self.stream
            .write_all(format!("get {key}\r\n").as_bytes())
            .await?;
        self.stream.flush().await?;
        self.line.clear();
        self.stream.read_line(&mut self.line).await?;
        if self.line.starts_with("END\r\n") {
            return Ok(None);
        }
        if !self.line.starts_with("VALUE ") {
            return Err(anyhow!("unexpected: {}", self.line.trim_end()));
        }
        // VALUE <key> <flags> <bytes> [<cas>]\r\n
        let parts: Vec<&str> = self.line.trim_end().split_whitespace().collect();
        let bytes_len: usize = parts
            .get(3)
            .ok_or_else(|| anyhow!("malformed VALUE: {}", self.line))?
            .parse()
            .context("VALUE size")?;
        // Skip <bytes_len> bytes + trailing \r\n.
        let mut data = vec![0u8; bytes_len + 2];
        self.stream.read_exact(&mut data).await?;
        // Then "END\r\n".
        self.line.clear();
        self.stream.read_line(&mut self.line).await?;
        Ok(Some(bytes_len as u64))
    }

    pub async fn set(&mut self, key: &str, flags: u32, exptime: u32, value: &[u8]) -> Result<()> {
        let header = format!("set {} {} {} {}\r\n", key, flags, exptime, value.len());
        self.stream.write_all(header.as_bytes()).await?;
        self.stream.write_all(value).await?;
        self.stream.write_all(b"\r\n").await?;
        self.stream.flush().await?;
        self.line.clear();
        self.stream.read_line(&mut self.line).await?;
        if self.line.starts_with("STORED") {
            Ok(())
        } else {
            Err(anyhow!("set: {}", self.line.trim_end()))
        }
    }

    pub async fn delete(&mut self, key: &str) -> Result<bool> {
        self.stream
            .write_all(format!("delete {key}\r\n").as_bytes())
            .await?;
        self.stream.flush().await?;
        self.line.clear();
        self.stream.read_line(&mut self.line).await?;
        Ok(self.line.starts_with("DELETED"))
    }
}

pub async fn setup_sandbox(cfg: &McConfig, client: &mut McClient) -> Result<()> {
    let mut value = vec![0u8; cfg.seed_value_size];
    rand::thread_rng().fill(&mut value[..]);
    for i in 0..cfg.seed_keys {
        let key = format!("{}{i}", cfg.prefix);
        client.set(&key, 0, cfg.seed_ttl, &value).await?;
    }
    Ok(())
}

/// Best-effort cleanup. memcached has no SCAN, so we walk our known
/// prefix-indexed keys and DELETE each.
pub async fn cleanup_sandbox(cfg: &McConfig) -> Result<u64> {
    let mut client = McClient::connect(&cfg.host, cfg.port).await?;
    let mut deleted = 0u64;
    for i in 0..cfg.seed_keys {
        let key = format!("{}{i}", cfg.prefix);
        if client.delete(&key).await.unwrap_or(false) {
            deleted += 1;
        }
    }
    Ok(deleted)
}

pub fn spawn_workers(
    cfg: Arc<McConfig>,
    connections: usize,
    h: WorkerHandles,
) -> Vec<JoinHandle<WorkerReport>> {
    (0..connections)
        .map(|i| {
            let cfg = cfg.clone();
            let h = h.clone();
            tokio::spawn(async move { run_worker(cfg, h, i as u64).await })
        })
        .collect()
}

async fn run_worker(cfg: Arc<McConfig>, h: WorkerHandles, worker_idx: u64) -> WorkerReport {
    let mut report = WorkerReport::new();
    let mut client = match McClient::connect(&cfg.host, cfg.port).await {
        Ok(c) => c,
        Err(e) => {
            report.record_error(&h.live, &format!("connect: {}", e));
            return report;
        }
    };

    let mut rng = SmallRng::seed_from_u64(0xCAFEBABE ^ worker_idx);

    while !h.stop.load(Ordering::Relaxed) {
        let measuring = h.measuring();
        if measuring && try_reserve_budget(&h.remaining).is_none() {
            break;
        }
        h.rate_gate().await;

        let outcome = if let Some(custom) = &cfg.custom {
            run_custom(&mut client, custom, cfg.timeout).await
        } else {
            run_workload(&mut client, &cfg, &mut rng).await
        };

        if !measuring {
            if outcome.is_err() {
                if let Ok(c) = McClient::connect(&cfg.host, cfg.port).await {
                    client = c;
                }
            }
            continue;
        }
        match outcome {
            Ok((op, latency_us, bytes)) => {
                report.record_op(&op, latency_us);
                h.live.requests.fetch_add(1, Ordering::Relaxed);
                h.live.bytes.fetch_add(bytes, Ordering::Relaxed);
                h.live.record_request_phases(latency_us, 0);
            }
            Err(e) => {
                report.record_error(&h.live, &format!("memcache: {}", e));
                if let Ok(c) = McClient::connect(&cfg.host, cfg.port).await {
                    client = c;
                }
            }
        }
    }
    report
}

async fn run_workload(
    client: &mut McClient,
    cfg: &McConfig,
    rng: &mut SmallRng,
) -> Result<(String, u64, u64)> {
    let pick_get = match cfg.workload {
        McWorkload::Read => true,
        McWorkload::Write => false,
        McWorkload::Memtier => rng.gen_range(0..11) != 0,
        McWorkload::Mixed => rng.gen_range(0..2) == 0,
    };
    let key_idx = rng.gen_range(0..cfg.seed_keys.max(1));
    let key = format!("{}{}", cfg.prefix, key_idx);

    let start = Instant::now();
    let (op, bytes) = if pick_get {
        let len = with_timeout(client.get(&key), cfg.timeout).await?.unwrap_or(0);
        ("GET", len)
    } else {
        let mut value = [0u8; 32];
        rng.fill(&mut value[..]);
        with_timeout(
            client.set(&key, 0, cfg.seed_ttl, &value),
            cfg.timeout,
        )
        .await?;
        ("SET", value.len() as u64)
    };
    Ok((op.to_string(), start.elapsed().as_micros() as u64, bytes))
}

async fn run_custom(
    client: &mut McClient,
    cmd: &str,
    timeout: Duration,
) -> Result<(String, u64, u64)> {
    let cmd = cmd.trim();
    let head = cmd.split_whitespace().next().unwrap_or("?").to_ascii_uppercase();
    let start = Instant::now();
    if let Some(rest) = cmd.strip_prefix("get ").or_else(|| cmd.strip_prefix("GET ")) {
        let key = rest.trim();
        let _ = with_timeout(client.get(key), timeout).await?;
    } else if let Some(rest) = cmd.strip_prefix("delete ").or_else(|| cmd.strip_prefix("DELETE ")) {
        let key = rest.trim();
        let _ = with_timeout(client.delete(key), timeout).await?;
    } else {
        return Err(anyhow!(
            "custom command must start with `get <key>` or `delete <key>` (set requires a value)"
        ));
    }
    Ok((head, start.elapsed().as_micros() as u64, 0))
}

async fn with_timeout<F, T>(fut: F, timeout: Duration) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(r) => r,
        Err(_) => Err(anyhow!("timeout")),
    }
}
