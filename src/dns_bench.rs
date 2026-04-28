//! DNS lookup benchmark. Each probe = one `getaddrinfo` via the system
//! resolver. Bundles a port placeholder so `lookup_host` accepts it.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use tokio::net::lookup_host;
use tokio::task::JoinHandle;

use crate::stats::{PhaseKind, WorkerReport};
use crate::worker::{try_reserve_budget, WorkerHandles};

#[derive(Clone, Debug)]
pub struct DnsConfig {
    pub name: String,
    pub timeout: Duration,
}

pub fn spawn_workers(
    cfg: Arc<DnsConfig>,
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

async fn run_worker(cfg: Arc<DnsConfig>, h: WorkerHandles) -> WorkerReport {
    let mut report = WorkerReport::new();
    while !h.stop.load(Ordering::Relaxed) {
        if try_reserve_budget(&h.remaining).is_none() {
            break;
        }
        h.rate_gate().await;
        match probe(&cfg).await {
            Ok(dns_us) => {
                report.record_phase(PhaseKind::Dns, dns_us);
                report.record_probe(dns_us);
                h.live.requests.fetch_add(1, Ordering::Relaxed);
                h.live.record_phase_live(PhaseKind::Dns, dns_us);
            }
            Err(e) => {
                report.record_error(&h.live, &format!("dns: {}", e));
            }
        }
    }
    report
}

async fn probe(cfg: &DnsConfig) -> Result<u64> {
    let dns_start = Instant::now();
    let lookup = lookup_host((cfg.name.as_str(), 0u16));
    let addrs = match tokio::time::timeout(cfg.timeout, lookup).await {
        Ok(r) => r?,
        Err(_) => return Err(anyhow!("timeout")),
    };
    // Force iteration so we count any per-record work as part of the lookup.
    let count = addrs.count();
    if count == 0 {
        return Err(anyhow!("no addresses for {}", cfg.name));
    }
    Ok(dns_start.elapsed().as_micros() as u64)
}
