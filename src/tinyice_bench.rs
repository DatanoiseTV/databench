//! TinyIce / Icecast2 benchmark.
//!
//! Three modes covering the load shapes a streaming server actually sees:
//!
//! - **listen**  N workers each open `GET /<mount>` and drain the audio
//!               stream forever. Measures connect time, time-to-first-byte,
//!               sustained bytes/sec per listener and aggregate, and counts
//!               disconnects mid-stream. The natural answer to "how many
//!               concurrent listeners does the server hold?".
//! - **source**  N workers open `SOURCE /<mount>` (Icecast2 ingest) with
//!               HTTP Basic auth, then push pseudo-audio bytes at
//!               `--source-bitrate` kbps using a paced writer. Measures
//!               connect+auth latency and aggregate ingest throughput.
//! - **mixed**   `--sources` source workers come up first (one per mount,
//!               round-robin), then `-c` listener workers spread across the
//!               mounts. The realistic radio-station workload.
//!
//! All workers respect `--qps`, `--warmup`, the global stop signal, and the
//! per-probe timeout. The `connections` live counter is repurposed here as
//! "currently connected" (incremented on a successful handshake,
//! decremented when the connection ends) so the dashboard reflects what an
//! operator cares about — how many listeners are actually streaming.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use base64::Engine;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufStream};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_rustls::TlsConnector;

use crate::stats::WorkerReport;
use crate::tls;
use crate::worker::WorkerHandles;

/// Type-erased duplex stream so the same code paths handle both plain
/// TCP and TLS-wrapped connections.
trait Wire: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + ?Sized> Wire for T {}

type IoStream = Box<dyn Wire>;

/// 60 seconds of a linear sine sweep from 200 Hz to 8 kHz, encoded as
/// 128 kbps stereo MP3 by `ffmpeg ... aevalsrc=0.5*sin(...) ...
/// -c:a libmp3lame`. Source workers cycle through these bytes in order
/// (paced at 128 kbps) so every byte on the wire is part of a
/// well-formed MPEG audio stream that real demuxers — and the server's
/// content-type sniff — accept. The 60 s period is also longer than any
/// reasonable Icecast burst buffer, which is what lets a future
/// FFT-based listener uniquely map an observed pitch back to a source
/// emission time without cycle ambiguity.
const SWEEP_60S_128K_MP3: &[u8] = include_bytes!("../assets/sweep-60s-128k.mp3");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TinyIceMode {
    Listen,
    Source,
    Mixed,
}

impl TinyIceMode {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "listen" | "listener" | "listeners" | "client" => Ok(Self::Listen),
            "source" | "sources" | "ingest" => Ok(Self::Source),
            "mixed" | "both" => Ok(Self::Mixed),
            other => Err(anyhow!(
                "unknown mode '{other}'; expected listen | source | mixed"
            )),
        }
    }
}

#[derive(Clone)]
pub struct TinyIceConfig {
    pub host: String,
    pub port: u16,
    pub use_tls: bool,
    pub insecure: bool,
    pub mounts: Vec<String>,
    pub mode: TinyIceMode,
    pub source_user: String,
    pub source_password: String,
    pub source_bitrate_kbps: u32,
    pub timeout: Duration,
}

pub fn spawn(
    cfg: Arc<TinyIceConfig>,
    listener_count: usize,
    source_count: usize,
    h: WorkerHandles,
) -> Vec<JoinHandle<WorkerReport>> {
    let mut handles = Vec::new();
    match cfg.mode {
        TinyIceMode::Listen => {
            for i in 0..listener_count {
                handles.push(spawn_listener(cfg.clone(), i, h.clone()));
            }
        }
        TinyIceMode::Source => {
            for i in 0..source_count {
                handles.push(spawn_source(cfg.clone(), i, h.clone()));
            }
        }
        TinyIceMode::Mixed => {
            for i in 0..source_count {
                handles.push(spawn_source(cfg.clone(), i, h.clone()));
            }
            // Tiny grace so sources have a chance to connect before listeners
            // start hammering — listeners against a brand-new mount with no
            // source yet would just churn.
            for i in 0..listener_count {
                let cfg = cfg.clone();
                let h = h.clone();
                handles.push(tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    listener_worker(cfg, i, h).await
                }));
            }
        }
    }
    handles
}

fn spawn_listener(
    cfg: Arc<TinyIceConfig>,
    idx: usize,
    h: WorkerHandles,
) -> JoinHandle<WorkerReport> {
    tokio::spawn(async move { listener_worker(cfg, idx, h).await })
}

fn spawn_source(
    cfg: Arc<TinyIceConfig>,
    idx: usize,
    h: WorkerHandles,
) -> JoinHandle<WorkerReport> {
    tokio::spawn(async move { source_worker(cfg, idx, h).await })
}

fn pick_mount<'a>(mounts: &'a [String], idx: usize) -> &'a str {
    let len = mounts.len().max(1);
    &mounts[idx % len]
}

async fn listener_worker(cfg: Arc<TinyIceConfig>, idx: usize, h: WorkerHandles) -> WorkerReport {
    let mut report = WorkerReport::new();
    let mount = pick_mount(&cfg.mounts, idx).to_string();

    while !h.stop.load(Ordering::Relaxed) {
        let measuring = h.measuring();
        h.rate_gate().await;

        let connect_start = Instant::now();
        let attempt = match h.race(cfg.timeout, listener_connect(&cfg, &mount)).await {
            crate::worker::RaceResult::Ok(Ok(stream_pair)) => Some(stream_pair),
            crate::worker::RaceResult::Ok(Err(e)) => {
                if measuring {
                    report.record_error(&h.live, &format!("listen: {}", short(&e)));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            crate::worker::RaceResult::Timeout => {
                if measuring {
                    report.record_error(&h.live, "listen: connect timeout");
                }
                continue;
            }
            crate::worker::RaceResult::Stopped => break,
        };
        let Some((mut bs, ttfb_us)) = attempt else { continue };
        let connect_us = connect_start.elapsed().as_micros() as u64;

        if measuring {
            report.record_op("listen-connect", connect_us);
            h.live.requests.fetch_add(1, Ordering::Relaxed);
            h.live.record_request_phases(ttfb_us, 0);
            h.live.connections.fetch_add(1, Ordering::Relaxed);
        }

        // Drain forever (or until stop/disconnect).
        let drain_res = drain_listener(&mut bs, &h, measuring).await;
        if measuring {
            h.live.connections.fetch_sub(1, Ordering::Relaxed);
            if let Err(e) = drain_res {
                report.record_error(&h.live, &format!("listen-drop: {}", e));
            }
        }
        if !h.stop.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    report
}

async fn drain_listener(
    bs: &mut BufStream<IoStream>,
    h: &WorkerHandles,
    measuring: bool,
) -> Result<()> {
    let mut buf = vec![0u8; 8192];
    while !h.stop.load(Ordering::Relaxed) {
        let read = h.race_stop(bs.read(&mut buf)).await;
        let n = match read {
            None => return Ok(()), // stop fired
            Some(Ok(0)) => return Err(anyhow!("eof")),
            Some(Ok(n)) => n,
            Some(Err(e)) => return Err(anyhow!("read: {}", e)),
        };
        if measuring {
            h.live.bytes.fetch_add(n as u64, Ordering::Relaxed);
        }
    }
    Ok(())
}

/// Dial a host:port — plain TCP or TLS-wrapped — and return a type-erased
/// async duplex stream. TLS uses `crate::tls::build_connector` so the
/// rustls config is consistent with the rest of databench.
async fn dial(cfg: &TinyIceConfig) -> Result<IoStream> {
    let tcp = TcpStream::connect((cfg.host.as_str(), cfg.port))
        .await
        .with_context(|| format!("connect {}:{}", cfg.host, cfg.port))?;
    let _ = tcp.set_nodelay(true);
    if !cfg.use_tls {
        return Ok(Box::new(tcp));
    }
    let connector: TlsConnector = tls::build_connector(cfg.insecure, false, true)
        .context("tls connector")?;
    let server_name = tls::server_name(&cfg.host)?;
    let tls_stream = connector.connect(server_name, tcp).await.context("TLS handshake")?;
    Ok(Box::new(tls_stream))
}

/// Open a listener connection: dial + HTTP request, parse status + headers,
/// confirm the first audio byte (TTFB), return the open stream.
async fn listener_connect(cfg: &TinyIceConfig, mount: &str) -> Result<(BufStream<IoStream>, u64)> {
    let host = cfg.host.as_str();
    let port = cfg.port;
    let stream = dial(cfg).await?;
    let mut bs: BufStream<IoStream> = BufStream::new(stream);

    let req = format!(
        "GET {mount} HTTP/1.1\r\n\
         Host: {host}:{port}\r\n\
         User-Agent: databench-tinyice/1\r\n\
         Icy-MetaData: 0\r\n\
         Accept: */*\r\n\
         Connection: keep-alive\r\n\
         \r\n"
    );
    bs.write_all(req.as_bytes()).await?;
    bs.flush().await?;

    let send_done = Instant::now();

    // Status line
    let mut line = String::new();
    bs.read_line(&mut line).await?;
    let trimmed = line.trim_end();
    let status: u16 = trimmed
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow!("malformed response: {trimmed}"))?;
    if status != 200 {
        return Err(anyhow!("http {} ({})", status, trimmed));
    }
    // Drain headers.
    loop {
        line.clear();
        let n = bs.read_line(&mut line).await?;
        if n == 0 || line == "\r\n" || line.trim().is_empty() {
            break;
        }
    }

    // Pull one byte to make sure the stream actually started.
    let mut byte = [0u8; 1];
    bs.read_exact(&mut byte).await?;
    let ttfb_us = send_done.elapsed().as_micros() as u64;

    Ok((bs, ttfb_us))
}

async fn source_worker(cfg: Arc<TinyIceConfig>, idx: usize, h: WorkerHandles) -> WorkerReport {
    let mut report = WorkerReport::new();
    let mount = pick_mount(&cfg.mounts, idx).to_string();

    let bytes_per_sec = (cfg.source_bitrate_kbps as f64 * 1000.0 / 8.0).max(1.0);
    let chunk_size = 1024usize;
    let interval_per_chunk = chunk_size as f64 / bytes_per_sec;

    while !h.stop.load(Ordering::Relaxed) {
        let measuring = h.measuring();
        h.rate_gate().await;

        let connect_start = Instant::now();
        let conn = match h.race(cfg.timeout, source_connect(&cfg, &mount)).await {
            crate::worker::RaceResult::Ok(Ok(c)) => Some(c),
            crate::worker::RaceResult::Ok(Err(e)) => {
                if measuring {
                    report.record_error(&h.live, &format!("source: {}", short(&e)));
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            crate::worker::RaceResult::Timeout => {
                if measuring {
                    report.record_error(&h.live, "source: connect timeout");
                }
                continue;
            }
            crate::worker::RaceResult::Stopped => break,
        };
        let Some(mut bs) = conn else { continue };
        let connect_us = connect_start.elapsed().as_micros() as u64;
        if measuring {
            report.record_op("source-connect", connect_us);
            h.live.requests.fetch_add(1, Ordering::Relaxed);
            h.live.record_request_phases(connect_us, 0);
            h.live.connections.fetch_add(1, Ordering::Relaxed);
        }

        // Cycle through the embedded sweep MP3 sequentially. Wraparound is
        // a hard byte cut at end-of-file — MP3 demuxers resync at the next
        // 0xFFFB sync word so a glitch every 60 s is fine for a benchmark.
        let payload = SWEEP_60S_128K_MP3;
        let mut offset = 0usize;
        let mut next_at = Instant::now();
        loop {
            if h.stop.load(Ordering::Relaxed) {
                break;
            }
            next_at += Duration::from_secs_f64(interval_per_chunk);
            tokio::select! {
                biased;
                _ = h.stop_notify.notified() => break,
                _ = tokio::time::sleep_until(next_at.into()) => {}
            }
            let end = (offset + chunk_size).min(payload.len());
            let chunk = &payload[offset..end];
            offset = if end == payload.len() { 0 } else { end };

            match bs.write_all(chunk).await {
                Ok(()) => {
                    if let Err(e) = bs.flush().await {
                        if measuring {
                            report.record_error(&h.live, &format!("source-flush: {}", e));
                        }
                        break;
                    }
                    if measuring {
                        h.live.bytes.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    if measuring {
                        report.record_error(&h.live, &format!("source-drop: {}", e));
                    }
                    break;
                }
            }
        }

        if measuring {
            h.live.connections.fetch_sub(1, Ordering::Relaxed);
        }
        if !h.stop.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    report
}

async fn source_connect(cfg: &TinyIceConfig, mount: &str) -> Result<BufStream<IoStream>> {
    let host = cfg.host.as_str();
    let port = cfg.port;
    let stream = dial(cfg).await?;
    let mut bs: BufStream<IoStream> = BufStream::new(stream);

    let auth = format!("{}:{}", cfg.source_user, cfg.source_password);
    let b64 = base64::engine::general_purpose::STANDARD.encode(auth);
    let bitrate = cfg.source_bitrate_kbps;

    let req = format!(
        "SOURCE {mount} HTTP/1.0\r\n\
         Host: {host}:{port}\r\n\
         Authorization: Basic {b64}\r\n\
         User-Agent: databench-tinyice/1\r\n\
         Content-Type: audio/mpeg\r\n\
         Ice-Name: databench load test\r\n\
         Ice-Description: synthetic\r\n\
         Ice-Genre: noise\r\n\
         Ice-Public: 0\r\n\
         Ice-Bitrate: {bitrate}\r\n\
         \r\n"
    );
    bs.write_all(req.as_bytes()).await?;
    bs.flush().await?;

    let mut line = String::new();
    bs.read_line(&mut line).await?;
    let trimmed = line.trim_end();
    let status: u16 = trimmed
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow!("malformed response: {trimmed}"))?;
    if status != 200 {
        return Err(anyhow!("http {} ({})", status, trimmed));
    }
    loop {
        line.clear();
        let n = bs.read_line(&mut line).await?;
        if n == 0 || line == "\r\n" || line.trim().is_empty() {
            break;
        }
    }
    Ok(bs)
}

fn short(e: &anyhow::Error) -> String {
    e.to_string().lines().next().unwrap_or("error").to_string()
}
