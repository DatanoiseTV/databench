//! Redis benchmark.
//!
//! Two operating modes:
//!
//! 1. **Sandbox (default)** — pick a unique key prefix `databench:<run-id>:`,
//!    seed N keys with values, then run a workload (read / mixed / write)
//!    using only those keys. Cleanup at the end deletes only keys that
//!    match our exact prefix. The sandbox uses a non-default DB (15 by
//!    default) so even partial cleanup never touches user data.
//!
//! 2. **Custom** — `--cmd "<command>"` runs that exact Redis command on
//!    every probe. The user owns the target.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use redis::{aio::MultiplexedConnection, AsyncCommands, Client};
use tokio::task::JoinHandle;

use crate::stats::WorkerReport;
use crate::worker::{try_reserve_budget, WorkerHandles};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RedisWorkload {
    /// 100% GET.
    Read,
    /// 80% GET, 15% SET, 5% INCR — typical cache pattern.
    Mixed,
    /// 100% SET.
    Write,
}

impl RedisWorkload {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "read" | "get" | "get-only" => Ok(Self::Read),
            "mixed" | "mix" => Ok(Self::Mixed),
            "write" | "set" | "set-only" => Ok(Self::Write),
            other => Err(anyhow!(
                "unknown workload '{other}'; expected read, mixed, or write"
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RedisConfig {
    pub url: String,
    pub prefix: String,
    pub workload: RedisWorkload,
    pub seed_keys: usize,
    pub seed_value_size: usize,
    pub seed_ttl: Duration,
    pub timeout: Duration,
    /// Custom command to run on every probe instead of the workload mix.
    /// First word is the command, the rest are arguments.
    pub custom: Option<Vec<String>>,
}

/// Build a `redis://` URL from CLI bits. If `target` already looks like a
/// URL we trust it; otherwise we glue `host[:port]`, optional `user:pass@`
/// and `/db` together.
pub fn build_url(
    target: &str,
    db: Option<u8>,
    user: Option<&str>,
    pass: Option<&str>,
    tls: bool,
) -> String {
    if target.starts_with("redis://") || target.starts_with("rediss://") {
        return target.to_string();
    }
    let scheme = if tls { "rediss" } else { "redis" };
    let auth = match (user, pass) {
        (Some(u), Some(p)) => format!("{u}:{p}@"),
        (None, Some(p)) => format!(":{p}@"),
        (Some(u), None) => format!("{u}@"),
        _ => String::new(),
    };
    let db_part = db.map(|d| format!("/{d}")).unwrap_or_default();
    format!("{scheme}://{auth}{target}{db_part}")
}

pub async fn connect(url: &str) -> Result<(Client, MultiplexedConnection)> {
    let client = Client::open(url).context("invalid redis URL")?;
    let conn = client
        .get_multiplexed_async_connection()
        .await
        .context("connecting to redis")?;
    Ok((client, conn))
}

/// Smoke-test the connection and warn the user if the server has an
/// eviction policy that could push out *their* keys when we seed ours.
pub async fn pre_run_checks(conn: &mut MultiplexedConnection) -> Result<()> {
    let info: String = redis::cmd("INFO")
        .arg("memory")
        .query_async(conn)
        .await
        .context("INFO memory")?;
    for line in info.lines() {
        if let Some(policy) = line.strip_prefix("maxmemory_policy:") {
            let policy = policy.trim();
            if policy != "noeviction" {
                eprintln!(
                    "warning: redis maxmemory_policy is '{policy}'. Seeding our \
                     keys may evict user data even with a sandboxed prefix."
                );
            }
            break;
        }
    }
    Ok(())
}

/// Populate `cfg.seed_keys` keys with `cfg.seed_value_size`-byte values and
/// `cfg.seed_ttl` TTL. The TTL is a safety net: if cleanup fails, the keys
/// expire on their own.
pub async fn setup_sandbox(cfg: &RedisConfig, conn: &mut MultiplexedConnection) -> Result<()> {
    let mut value = vec![0u8; cfg.seed_value_size];
    rand::thread_rng().fill(&mut value[..]);
    let ttl_secs = cfg.seed_ttl.as_secs() as u64;
    // Use a pipeline so the seed phase doesn't itself dominate the wall clock.
    let chunk_size = 1000usize;
    let mut written = 0usize;
    while written < cfg.seed_keys {
        let end = (written + chunk_size).min(cfg.seed_keys);
        let mut pipe = redis::pipe();
        for i in written..end {
            pipe.cmd("SET")
                .arg(format!("{}{i}", cfg.prefix))
                .arg(&value[..])
                .arg("EX")
                .arg(ttl_secs)
                .ignore();
        }
        let _: () = pipe.query_async(conn).await.context("seed pipeline")?;
        written = end;
    }
    Ok(())
}

/// Delete every key matching our prefix. Uses SCAN+DEL in batches so we
/// never call `KEYS *` and never block a busy server.
pub async fn cleanup_sandbox(cfg: &RedisConfig, conn: &mut MultiplexedConnection) -> Result<u64> {
    let pattern = format!("{}*", cfg.prefix);
    let mut cursor: u64 = 0;
    let mut deleted: u64 = 0;
    loop {
        let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(500)
            .query_async(conn)
            .await
            .context("SCAN")?;
        if !batch.is_empty() {
            // Defensive: refuse to delete anything not actually matching our prefix.
            let safe: Vec<&String> = batch
                .iter()
                .filter(|k| k.starts_with(&cfg.prefix))
                .collect();
            if !safe.is_empty() {
                let n: u64 = redis::cmd("DEL")
                    .arg(safe)
                    .query_async(conn)
                    .await
                    .context("DEL")?;
                deleted += n;
            }
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(deleted)
}

pub fn spawn_workers(
    cfg: Arc<RedisConfig>,
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

async fn run_worker(cfg: Arc<RedisConfig>, h: WorkerHandles, worker_idx: u64) -> WorkerReport {
    let mut report = WorkerReport::new();
    let mut conn = match connect(&cfg.url).await {
        Ok((_c, conn)) => conn,
        Err(e) => {
            report.record_error(&h.live, &format!("connect: {}", e));
            return report;
        }
    };

    let mut rng = SmallRng::seed_from_u64(0xC0FFEE_DECADE ^ worker_idx);
    let prefix = cfg.prefix.as_str();

    // Pre-tokenise a custom command so we don't reparse it on every probe.
    let custom_args: Option<(String, Vec<String>)> = cfg.custom.as_ref().and_then(|args| {
        let mut iter = args.iter().cloned();
        let head = iter.next()?;
        Some((head, iter.collect()))
    });

    while !h.stop.load(Ordering::Relaxed) {
        if try_reserve_budget(&h.remaining).is_none() {
            break;
        }
        h.rate_gate().await;

        let outcome = if let Some((cmd, args)) = &custom_args {
            run_custom(&mut conn, cmd, args, cfg.timeout).await
        } else {
            run_workload(&mut conn, &cfg.workload, prefix, cfg.seed_keys, &mut rng, cfg.timeout)
                .await
        };

        match outcome {
            Ok(OpResult { op, latency_us, bytes }) => {
                report.record_op(&op, latency_us);
                h.live.requests.fetch_add(1, Ordering::Relaxed);
                h.live.bytes.fetch_add(bytes, Ordering::Relaxed);
                h.live.record_request_phases(latency_us, 0);
            }
            Err(e) => {
                report.record_error(&h.live, &format!("redis: {}", e));
                // try to reconnect
                if let Ok((_, c)) = connect(&cfg.url).await {
                    conn = c;
                }
            }
        }
    }
    report
}

struct OpResult {
    op: String,
    latency_us: u64,
    bytes: u64,
}

async fn run_workload(
    conn: &mut MultiplexedConnection,
    workload: &RedisWorkload,
    prefix: &str,
    seed_keys: usize,
    rng: &mut SmallRng,
    timeout: Duration,
) -> Result<OpResult> {
    let pick = match workload {
        RedisWorkload::Read => Op::Get,
        RedisWorkload::Write => Op::Set,
        RedisWorkload::Mixed => {
            let r: u32 = rng.gen_range(0..100);
            if r < 80 {
                Op::Get
            } else if r < 95 {
                Op::Set
            } else {
                Op::Incr
            }
        }
    };
    let key_idx: usize = rng.gen_range(0..seed_keys.max(1));
    let key = format!("{prefix}{key_idx}");

    let start = Instant::now();
    let bytes = match pick {
        Op::Get => {
            let v: Option<Vec<u8>> = run_with_timeout(conn.get::<_, Option<Vec<u8>>>(&key), timeout).await?;
            v.map(|x| x.len()).unwrap_or(0) as u64
        }
        Op::Set => {
            // 64-byte fixed payload — small enough not to dominate, big
            // enough to exercise serialization.
            let mut value = [0u8; 64];
            rng.fill(&mut value[..]);
            let _: () = run_with_timeout(conn.set::<_, _, ()>(&key, &value[..]), timeout).await?;
            value.len() as u64
        }
        Op::Incr => {
            let counter = format!("{prefix}counter:{}", key_idx % 16);
            let _: i64 = run_with_timeout(conn.incr::<_, _, i64>(&counter, 1i64), timeout).await?;
            0
        }
    };
    let latency_us = start.elapsed().as_micros() as u64;
    Ok(OpResult {
        op: pick.label().to_string(),
        latency_us,
        bytes,
    })
}

async fn run_custom(
    conn: &mut MultiplexedConnection,
    cmd: &str,
    args: &[String],
    timeout: Duration,
) -> Result<OpResult> {
    let mut c = redis::cmd(cmd);
    for a in args {
        c.arg(a.as_str());
    }
    let start = Instant::now();
    let _v: redis::Value = run_with_timeout(c.query_async(conn), timeout).await?;
    let latency_us = start.elapsed().as_micros() as u64;
    Ok(OpResult {
        op: cmd.to_ascii_uppercase(),
        latency_us,
        bytes: 0,
    })
}

async fn run_with_timeout<F, T, E>(fut: F, timeout: Duration) -> Result<T>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(anyhow!("{}", e)),
        Err(_) => Err(anyhow!("timeout")),
    }
}

#[derive(Copy, Clone)]
enum Op {
    Get,
    Set,
    Incr,
}

impl Op {
    fn label(self) -> &'static str {
        match self {
            Op::Get => "GET",
            Op::Set => "SET",
            Op::Incr => "INCR",
        }
    }
}
