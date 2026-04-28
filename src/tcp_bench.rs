//! TCP connect-time benchmark. Each probe = one fresh TCP handshake to
//! `host:port`. Measures DNS lookup time and TCP connect time per probe.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use tokio::net::{lookup_host, TcpStream};
use tokio::task::JoinHandle;

use crate::stats::{PhaseKind, WorkerReport};
use crate::worker::{try_reserve_budget, WorkerHandles};

#[derive(Clone, Debug)]
pub struct TcpConfig {
    pub host: String,
    pub port: u16,
    pub timeout: Duration,
}

pub fn spawn_workers(
    cfg: Arc<TcpConfig>,
    connections: usize,
    h: WorkerHandles,
) -> Vec<JoinHandle<WorkerReport>> {
    (0..connections)
        .map(|_| {
            let cfg = cfg.clone();
            let h = h.clone();
            tokio::spawn(async move { run_worker(cfg, h).await })
        })
        .collect()
}

async fn run_worker(cfg: Arc<TcpConfig>, h: WorkerHandles) -> WorkerReport {
    let mut report = WorkerReport::new();
    while !h.stop.load(Ordering::Relaxed) {
        if try_reserve_budget(&h.remaining).is_none() {
            break;
        }
        match probe(&cfg).await {
            Ok((dns_us, tcp_us)) => {
                let total = dns_us + tcp_us;
                report.record_phase(PhaseKind::Dns, dns_us);
                report.record_phase(PhaseKind::Tcp, tcp_us);
                report.record_probe(total);
                h.live.requests.fetch_add(1, Ordering::Relaxed);
                h.live.record_phase_live(PhaseKind::Dns, dns_us);
                h.live.record_phase_live(PhaseKind::Tcp, tcp_us);
            }
            Err(e) => {
                report.record_error(&h.live, &format!("tcp: {}", e));
            }
        }
    }
    report
}

async fn probe(cfg: &TcpConfig) -> Result<(u64, u64)> {
    let dns_start = Instant::now();
    let mut addrs = lookup_host((cfg.host.as_str(), cfg.port)).await?;
    let addr = addrs
        .next()
        .ok_or_else(|| anyhow!("no addresses for {}", cfg.host))?;
    let dns_us = dns_start.elapsed().as_micros() as u64;

    let tcp_start = Instant::now();
    let connect_fut = TcpStream::connect(addr);
    let stream = match tokio::time::timeout(cfg.timeout, connect_fut).await {
        Ok(r) => r?,
        Err(_) => return Err(anyhow!("connect: timeout")),
    };
    let tcp_us = tcp_start.elapsed().as_micros() as u64;
    drop(stream);
    Ok((dns_us, tcp_us))
}
