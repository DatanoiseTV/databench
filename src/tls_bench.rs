//! TLS handshake-only benchmark. Each probe = TCP connect + TLS handshake
//! (no HTTP). Useful for measuring how fast a remote endpoint can complete
//! TLS sessions. Records DNS, TCP, and TLS phase timings.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use parking_lot::Mutex;
use tokio::net::{lookup_host, TcpStream};
use tokio::task::JoinHandle;
use tokio_rustls::TlsConnector;

use crate::stats::{PhaseKind, WorkerReport};
use crate::tls;
use crate::worker::{try_reserve_budget, WorkerHandles};

#[derive(Clone, Debug)]
pub struct TlsBenchConfig {
    pub host: String,
    pub port: u16,
    pub sni: Option<String>,
    pub insecure: bool,
    pub timeout: Duration,
}

pub fn spawn_workers(
    cfg: Arc<TlsBenchConfig>,
    connections: usize,
    h: WorkerHandles,
    proto_observed: Arc<Mutex<Option<&'static str>>>,
) -> Vec<JoinHandle<WorkerReport>> {
    (0..connections)
        .map(|_| {
            let cfg = cfg.clone();
            let h = h.clone();
            let proto = proto_observed.clone();
            tokio::spawn(async move { run_worker(cfg, h, proto).await })
        })
        .collect()
}

async fn run_worker(
    cfg: Arc<TlsBenchConfig>,
    h: WorkerHandles,
    proto_observed: Arc<Mutex<Option<&'static str>>>,
) -> WorkerReport {
    let mut report = WorkerReport::new();
    let connector = match tls::build_connector(cfg.insecure, false, true) {
        Ok(c) => c,
        Err(e) => {
            report.record_error(&h.live, &format!("tls config: {e}"));
            return report;
        }
    };
    while !h.stop.load(Ordering::Relaxed) {
        if try_reserve_budget(&h.remaining).is_none() {
            break;
        }
        match probe(&cfg, &connector).await {
            Ok((dns_us, tcp_us, tls_us, proto)) => {
                let total = dns_us + tcp_us + tls_us;
                report.record_phase(PhaseKind::Dns, dns_us);
                report.record_phase(PhaseKind::Tcp, tcp_us);
                report.record_phase(PhaseKind::Tls, tls_us);
                report.record_probe(total);
                h.live.requests.fetch_add(1, Ordering::Relaxed);
                h.live.record_phase_live(PhaseKind::Dns, dns_us);
                h.live.record_phase_live(PhaseKind::Tcp, tcp_us);
                h.live.record_phase_live(PhaseKind::Tls, tls_us);
                let mut g = proto_observed.lock();
                if g.is_none() {
                    *g = Some(proto);
                }
            }
            Err(e) => {
                report.record_error(&h.live, &format!("tls: {}", e));
            }
        }
    }
    report
}

async fn probe(
    cfg: &TlsBenchConfig,
    connector: &TlsConnector,
) -> Result<(u64, u64, u64, &'static str)> {
    let dns_start = Instant::now();
    let mut addrs = lookup_host((cfg.host.as_str(), cfg.port)).await?;
    let addr = addrs
        .next()
        .ok_or_else(|| anyhow!("no addresses for {}", cfg.host))?;
    let dns_us = dns_start.elapsed().as_micros() as u64;

    let tcp_start = Instant::now();
    let stream = match tokio::time::timeout(cfg.timeout, TcpStream::connect(addr)).await {
        Ok(r) => r?,
        Err(_) => return Err(anyhow!("connect: timeout")),
    };
    let tcp_us = tcp_start.elapsed().as_micros() as u64;
    let _ = stream.set_nodelay(true);

    let server_name_str = cfg.sni.clone().unwrap_or_else(|| cfg.host.clone());
    let sn = tls::server_name(&server_name_str)?;
    let tls_start = Instant::now();
    let tls_stream = match tokio::time::timeout(cfg.timeout, connector.connect(sn, stream)).await {
        Ok(r) => r?,
        Err(_) => return Err(anyhow!("tls: timeout")),
    };
    let tls_us = tls_start.elapsed().as_micros() as u64;
    let alpn = tls_stream.get_ref().1.alpn_protocol();
    let proto: &'static str = if alpn == Some(b"h2") {
        "TLS+ALPN h2"
    } else if alpn == Some(b"http/1.1") {
        "TLS+ALPN h1"
    } else {
        "TLS"
    };
    drop(tls_stream);
    Ok((dns_us, tcp_us, tls_us, proto))
}
