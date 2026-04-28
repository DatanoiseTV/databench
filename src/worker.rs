use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use http::{HeaderName, HeaderValue, Request, Uri};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::{http1, http2};
use hyper_util::rt::{TokioExecutor, TokioIo};
use parking_lot::Mutex;
use tokio::net::{lookup_host, TcpStream};
use tokio_rustls::TlsConnector;

use crate::ratelimit::RateLimiter;
use crate::stats::{LiveStats, PhaseConnect, WorkerReport};
use crate::tls;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    Http,
    Https,
}

pub struct Target {
    pub scheme: Scheme,
    pub host: String,
    pub port: u16,
    pub authority: String,
    pub uri: Uri,
    pub path: Uri,
}

pub struct WorkerConfig {
    pub target: Arc<Target>,
    pub method: http::Method,
    pub headers: Arc<Vec<(HeaderName, HeaderValue)>>,
    pub body: Bytes,
    pub timeout: Duration,
    pub keepalive: bool,
    pub force_h2: bool,
    pub insecure: bool,
}

#[derive(Clone)]
pub struct WorkerHandles {
    pub stop: Arc<AtomicBool>,
    /// Notify all waiters as soon as `stop` is set so workers stuck
    /// inside a slow probe await wake immediately instead of
    /// discovering `stop = true` only at the top of their loop.
    pub stop_notify: Arc<tokio::sync::Notify>,
    /// Remaining request budget. Negative = unbounded (duration mode).
    pub remaining: Arc<AtomicI64>,
    pub live: Arc<LiveStats>,
    /// Optional shared QPS gate. `None` = unthrottled.
    pub qps: Option<Arc<RateLimiter>>,
    /// True once the warmup period has ended (or immediately, if no
    /// warmup was configured). Workers run probes either way, but they
    /// neither consume budget nor record stats while this is false.
    pub recording: Arc<AtomicBool>,
}

/// Outcome of awaiting a probe alongside the global stop signal.
pub enum RaceResult<T> {
    /// Probe completed.
    Ok(T),
    /// Per-probe timeout expired.
    Timeout,
    /// `stop` was signalled — caller should exit the worker loop.
    Stopped,
}

impl WorkerHandles {
    /// Block until the rate limiter (if any) lets us through.
    pub async fn rate_gate(&self) {
        if let Some(rl) = &self.qps {
            rl.acquire().await;
        }
    }

    /// Whether workers should currently record stats / consume budget.
    /// During warmup this is false — probes still hit the server, but
    /// their latencies are discarded so the warm-cache distribution
    /// dominates the published numbers.
    #[inline]
    pub fn measuring(&self) -> bool {
        self.recording.load(Ordering::Relaxed)
    }

    /// Race `fut` against the stop signal and a per-probe timeout.
    /// `biased` ensures stop wins when both are ready (the user pressed
    /// Esc; we want out *now*, not after one more probe).
    pub async fn race<F, T>(&self, timeout: Duration, fut: F) -> RaceResult<T>
    where
        F: std::future::Future<Output = T>,
    {
        tokio::select! {
            biased;
            _ = self.stop_notify.notified() => RaceResult::Stopped,
            _ = tokio::time::sleep(timeout) => RaceResult::Timeout,
            v = fut => RaceResult::Ok(v),
        }
    }

    /// Race a future against just the stop signal — for awaits where
    /// the existing code already wraps in its own timeout.
    pub async fn race_stop<F, T>(&self, fut: F) -> Option<T>
    where
        F: std::future::Future<Output = T>,
    {
        tokio::select! {
            biased;
            _ = self.stop_notify.notified() => None,
            v = fut => Some(v),
        }
    }
}

/// Try to reserve one unit from the shared request budget. Returns
/// `Some(())` if the worker may proceed, `None` if the run is finished.
pub fn try_reserve_budget(remaining: &AtomicI64) -> Option<()> {
    loop {
        let prev = remaining.load(Ordering::Relaxed);
        if prev < 0 {
            return Some(()); // unbounded
        }
        if prev == 0 {
            return None;
        }
        if remaining
            .compare_exchange(prev, prev - 1, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return Some(());
        }
    }
}

enum EitherSender {
    Http1(http1::SendRequest<Full<Bytes>>),
    Http2(http2::SendRequest<Full<Bytes>>),
}

impl EitherSender {
    async fn ready(&mut self) -> Result<(), hyper::Error> {
        match self {
            EitherSender::Http1(s) => s.ready().await,
            EitherSender::Http2(s) => s.ready().await,
        }
    }

    async fn send(
        &mut self,
        req: Request<Full<Bytes>>,
    ) -> Result<hyper::Response<Incoming>, hyper::Error> {
        match self {
            EitherSender::Http1(s) => s.send_request(req).await,
            EitherSender::Http2(s) => s.send_request(req).await,
        }
    }

    fn is_h2(&self) -> bool {
        matches!(self, EitherSender::Http2(_))
    }
}

pub async fn run_worker(
    cfg: Arc<WorkerConfig>,
    h: WorkerHandles,
    proto_observed: Arc<Mutex<Option<&'static str>>>,
) -> WorkerReport {
    let mut report = WorkerReport::new();

    let tls_conn = if cfg.target.scheme == Scheme::Https {
        let allow_h1 = !cfg.force_h2;
        let prefer_h2 = true;
        match tls::build_connector(cfg.insecure, prefer_h2, allow_h1) {
            Ok(c) => Some(c),
            Err(e) => {
                report.record_error(&h.live, &format!("tls config: {e}"));
                return report;
            }
        }
    } else {
        None
    };

    'outer: while !h.stop.load(Ordering::Relaxed) {
        if h.remaining.load(Ordering::Relaxed) == 0 {
            break;
        }

        let connect_fut = connect(&cfg, tls_conn.as_ref());
        let conn_measuring = h.measuring();
        let (sender_built, phases) = match h.race(cfg.timeout, connect_fut).await {
            RaceResult::Ok(Ok(c)) => c,
            RaceResult::Ok(Err(e)) => {
                if conn_measuring {
                    report.record_error(&h.live, &format!("connect: {}", classify(&e)));
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }
            RaceResult::Timeout => {
                if conn_measuring {
                    report.record_error(&h.live, "connect: timeout");
                }
                continue;
            }
            RaceResult::Stopped => break 'outer,
        };
        if conn_measuring {
            report.record_connection(&phases);
            h.live.record_connection_phases(&phases);
        }

        {
            let mut g = proto_observed.lock();
            if g.is_none() {
                *g = Some(if sender_built.is_h2() { "HTTP/2" } else { "HTTP/1.1" });
            }
        }

        let mut sender = sender_built;
        let use_full_uri = sender.is_h2();
        let mut requests_on_conn: u64 = 0;

        loop {
            if h.stop.load(Ordering::Relaxed) {
                break 'outer;
            }

            // Wait for the connection to become writable. If it died on us
            // (e.g. server closed the connection after a HTTP/1.0 response,
            // or after Connection: close), silently reconnect — only count
            // an error if we never successfully exchanged a request on this
            // connection (truly broken peer).
            if let Err(e) = sender.ready().await {
                if requests_on_conn == 0 && h.measuring() {
                    report.record_error(&h.live, &format!("send-ready: {}", chain_msg(&e)));
                }
                continue 'outer;
            }

            let measuring = h.measuring();
            // Reserve a request slot only once we know the conn is writable
            // *and* we're past warmup. AtomicI64 < 0 means unbounded.
            if measuring {
                let prev = h.remaining.load(Ordering::Relaxed);
                if prev >= 0 {
                    if prev == 0 {
                        break 'outer;
                    }
                    if h
                        .remaining
                        .compare_exchange(prev, prev - 1, Ordering::Relaxed, Ordering::Relaxed)
                        .is_err()
                    {
                        continue;
                    }
                }
            }

            h.rate_gate().await;
            let req = build_request(&cfg, use_full_uri);
            let send_start = Instant::now();
            let resp = match h.race(cfg.timeout, sender.send(req)).await {
                RaceResult::Ok(Ok(r)) => r,
                RaceResult::Ok(Err(e)) => {
                    if measuring {
                        report.record_error(&h.live, &format!("send: {}", chain_msg(&e)));
                    }
                    continue 'outer;
                }
                RaceResult::Timeout => {
                    if measuring {
                        report.record_error(&h.live, "send: timeout");
                    }
                    continue 'outer;
                }
                RaceResult::Stopped => break 'outer,
            };
            // Response future resolves once response headers are received,
            // so this is the time to first byte.
            let ttfb_us = send_start.elapsed().as_micros() as u64;
            let status = resp.status().as_u16();

            let body_start = Instant::now();
            let body_bytes = match drain_body_with_timeout(resp.into_body(), cfg.timeout).await {
                Ok(b) => b,
                Err(e) => {
                    if measuring {
                        report.record_error(&h.live, &format!("body: {}", e));
                    }
                    continue 'outer;
                }
            };
            let body_us = body_start.elapsed().as_micros() as u64;
            let total_us = send_start.elapsed().as_micros() as u64;

            if measuring {
                report.record_request(ttfb_us, body_us, total_us, status, body_bytes);
                h.live.requests.fetch_add(1, Ordering::Relaxed);
                h.live.bytes.fetch_add(body_bytes, Ordering::Relaxed);
                h.live.record_status(status);
                h.live.record_request_phases(ttfb_us, body_us);
            }
            requests_on_conn += 1;

            if !cfg.keepalive {
                continue 'outer;
            }
        }
    }

    report
}

async fn connect(
    cfg: &WorkerConfig,
    tls_conn: Option<&TlsConnector>,
) -> Result<(EitherSender, PhaseConnect)> {
    // 1. DNS lookup
    let dns_start = Instant::now();
    let mut addrs = lookup_host((cfg.target.host.as_str(), cfg.target.port)).await?;
    let addr = addrs
        .next()
        .ok_or_else(|| anyhow!("no addresses for {}", cfg.target.host))?;
    let dns_us = dns_start.elapsed().as_micros() as u64;

    // 2. TCP connect
    let tcp_start = Instant::now();
    let stream = TcpStream::connect(addr).await?;
    let tcp_us = tcp_start.elapsed().as_micros() as u64;
    let _ = stream.set_nodelay(true);

    // 3. (HTTPS only) TLS handshake; 4. HTTP handshake
    match (cfg.target.scheme, tls_conn) {
        (Scheme::Https, Some(tc)) => {
            let sn = tls::server_name(&cfg.target.host)?;
            let tls_start = Instant::now();
            let tls = tc.connect(sn, stream).await?;
            let tls_us = tls_start.elapsed().as_micros() as u64;
            let alpn_h2 = tls.get_ref().1.alpn_protocol() == Some(b"h2");

            let hs_start = Instant::now();
            let sender = if alpn_h2 {
                let (sender, conn) =
                    http2::handshake(TokioExecutor::new(), TokioIo::new(tls)).await?;
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                EitherSender::Http2(sender)
            } else {
                let (sender, conn) = http1::handshake(TokioIo::new(tls)).await?;
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                EitherSender::Http1(sender)
            };
            let hs_us = hs_start.elapsed().as_micros() as u64;

            Ok((
                sender,
                PhaseConnect {
                    dns_us,
                    tcp_us,
                    tls_us: Some(tls_us),
                    hs_us,
                },
            ))
        }
        (Scheme::Http, _) => {
            let hs_start = Instant::now();
            let sender = if cfg.force_h2 {
                let (sender, conn) =
                    http2::handshake(TokioExecutor::new(), TokioIo::new(stream)).await?;
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                EitherSender::Http2(sender)
            } else {
                let (sender, conn) = http1::handshake(TokioIo::new(stream)).await?;
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                EitherSender::Http1(sender)
            };
            let hs_us = hs_start.elapsed().as_micros() as u64;

            Ok((
                sender,
                PhaseConnect {
                    dns_us,
                    tcp_us,
                    tls_us: None,
                    hs_us,
                },
            ))
        }
        _ => unreachable!("https without tls connector"),
    }
}

fn build_request(cfg: &WorkerConfig, full_uri: bool) -> Request<Full<Bytes>> {
    let uri = if full_uri {
        cfg.target.uri.clone()
    } else {
        cfg.target.path.clone()
    };
    let mut builder = Request::builder().method(cfg.method.clone()).uri(uri);
    let mut has_host = false;
    let mut has_ua = false;
    let mut has_cl = false;
    for (k, v) in cfg.headers.iter() {
        if *k == http::header::HOST {
            has_host = true;
        }
        if *k == http::header::USER_AGENT {
            has_ua = true;
        }
        if *k == http::header::CONTENT_LENGTH {
            has_cl = true;
        }
        builder = builder.header(k.clone(), v.clone());
    }
    if !has_host && !full_uri {
        builder = builder.header(http::header::HOST, cfg.target.authority.clone());
    }
    if !has_ua {
        builder = builder.header(http::header::USER_AGENT, "sbch/0.1");
    }
    if !cfg.body.is_empty() && !has_cl {
        builder = builder.header(http::header::CONTENT_LENGTH, cfg.body.len());
    }
    builder
        .body(Full::new(cfg.body.clone()))
        .expect("request build")
}

async fn drain_body_with_timeout(mut body: Incoming, timeout: Duration) -> Result<u64, String> {
    let fut = async {
        let mut total = 0u64;
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| chain_msg(&e))?;
            if let Ok(data) = frame.into_data() {
                total += data.len() as u64;
            }
        }
        Ok::<u64, String>(total)
    };
    match tokio::time::timeout(timeout, fut).await {
        Ok(r) => r,
        Err(_) => Err("timeout".to_string()),
    }
}

fn classify(e: &anyhow::Error) -> String {
    // Look for a useful io::ErrorKind, but fall through to the actual
    // message text whenever the kind would just be "other" or
    // "uncategorized" (which leaks no info).
    let mut src: Option<&(dyn std::error::Error + 'static)> = Some(e.as_ref());
    while let Some(s) = src {
        if let Some(io) = s.downcast_ref::<std::io::Error>() {
            let kind_str = format!("{:?}", io.kind()).to_lowercase();
            if kind_str != "other" && kind_str != "uncategorized" {
                return kind_str;
            }
            if let Some(inner) = io.get_ref() {
                return inner.to_string();
            }
            return io.to_string();
        }
        src = s.source();
    }
    chain_msg(e.as_ref())
}

/// Concatenates the error message and every cause in its source chain so
/// generic outer messages like "channel closed" are paired with the actual
/// cause underneath ("broken pipe", "tls handshake failed", etc.).
pub fn chain_msg(e: &(dyn std::error::Error + 'static)) -> String {
    let mut parts: Vec<String> = vec![e.to_string()];
    let mut current = e;
    while let Some(s) = current.source() {
        let msg = s.to_string();
        if parts.last().map(String::as_str) != Some(msg.as_str()) {
            parts.push(msg);
        }
        current = s;
    }
    parts.join(": ")
}
