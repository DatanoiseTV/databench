//! ICMP echo (ping) benchmark. Uses `surge-ping` with the DGRAM socket hint
//! so it does not require root on macOS. On Linux it requires either
//! `CAP_NET_RAW` or `sysctl -w net.ipv4.ping_group_range="0 65535"`.

use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use socket2::Type;
use surge_ping::{Client, Config, PingIdentifier, PingSequence, ICMP};
use tokio::net::lookup_host;
use tokio::task::JoinHandle;

use crate::stats::WorkerReport;
use crate::worker::{try_reserve_budget, WorkerHandles};

#[derive(Clone, Debug)]
pub struct PingConfig {
    pub host: String,
    pub addr: IpAddr,
    pub payload_size: usize,
    pub ttl: Option<u32>,
    pub timeout: Duration,
}

pub async fn resolve_target(host: &str, prefer_ipv6: bool) -> Result<IpAddr> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(ip);
    }
    let mut addrs: Vec<_> = lookup_host((host, 0u16)).await?.collect();
    if addrs.is_empty() {
        return Err(anyhow!("no addresses for {host}"));
    }
    addrs.sort_by_key(|a| match (prefer_ipv6, a.is_ipv6()) {
        (true, true) | (false, false) => 0,
        _ => 1,
    });
    Ok(addrs[0].ip())
}

pub fn build_client(ip: IpAddr, ttl: Option<u32>) -> Result<Client> {
    let kind = if ip.is_ipv4() { ICMP::V4 } else { ICMP::V6 };
    let mut builder = Config::builder().kind(kind);
    builder = builder.sock_type_hint(Type::DGRAM);
    if let Some(t) = ttl {
        builder = builder.ttl(t);
    }
    let config = builder.build();
    Client::new(&config).context("creating ICMP socket (try sudo / CAP_NET_RAW)")
}

pub fn spawn_workers(
    client: Arc<Client>,
    cfg: Arc<PingConfig>,
    connections: usize,
    h: WorkerHandles,
) -> Vec<JoinHandle<WorkerReport>> {
    (0..connections)
        .map(|i| {
            let client = client.clone();
            let cfg = cfg.clone();
            let h = h.clone();
            // Per-worker identifier so replies are correlated correctly even
            // when multiple workers ping the same host.
            let id = (rand::random::<u16>()).wrapping_add(i as u16);
            tokio::spawn(async move { run_worker(client, cfg, h, id).await })
        })
        .collect()
}

async fn run_worker(
    client: Arc<Client>,
    cfg: Arc<PingConfig>,
    h: WorkerHandles,
    id: u16,
) -> WorkerReport {
    let mut report = WorkerReport::new();
    let payload = vec![0u8; cfg.payload_size];

    let mut pinger = client.pinger(cfg.addr, PingIdentifier(id)).await;
    pinger.timeout(cfg.timeout);

    let mut seq: u16 = 0;
    while !h.stop.load(Ordering::Relaxed) {
        let measuring = h.measuring();
        if measuring && try_reserve_budget(&h.remaining).is_none() {
            break;
        }
        h.rate_gate().await;
        let start = Instant::now();
        let res = pinger.ping(PingSequence(seq), &payload).await;
        if measuring {
            match res {
                Ok((_packet, rtt)) => {
                    let rtt_us = rtt.as_micros() as u64;
                    report.record_probe(rtt_us);
                    h.live.requests.fetch_add(1, Ordering::Relaxed);
                    h.live.bytes.fetch_add(payload.len() as u64, Ordering::Relaxed);
                    h.live.record_request_phases(rtt_us, 0);
                }
                Err(e) => {
                    let _ = start;
                    report.record_error(&h.live, &format!("ping: {}", e));
                }
            }
        }
        seq = seq.wrapping_add(1);
    }
    report
}
