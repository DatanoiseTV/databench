use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hdrhistogram::Histogram;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchKind {
    Http,
    Ping,
    Tcp,
    Dns,
    Tls,
    Redis,
    Postgres,
    Mysql,
    Memcache,
}

impl BenchKind {
    pub fn label(self) -> &'static str {
        match self {
            BenchKind::Http => "HTTP",
            BenchKind::Ping => "ICMP ping",
            BenchKind::Tcp => "TCP connect",
            BenchKind::Dns => "DNS lookup",
            BenchKind::Tls => "TLS handshake",
            BenchKind::Redis => "Redis",
            BenchKind::Postgres => "PostgreSQL",
            BenchKind::Mysql => "MySQL",
            BenchKind::Memcache => "memcached",
        }
    }

    pub fn unit(self) -> &'static str {
        match self {
            BenchKind::Http => "req",
            BenchKind::Ping => "ping",
            BenchKind::Tcp | BenchKind::Tls => "conn",
            BenchKind::Dns => "lookup",
            BenchKind::Redis | BenchKind::Memcache => "op",
            BenchKind::Postgres | BenchKind::Mysql => "txn",
        }
    }

    pub fn unit_plural(self) -> &'static str {
        match self {
            BenchKind::Http => "reqs",
            BenchKind::Ping => "pings",
            BenchKind::Tcp => "conns",
            BenchKind::Dns => "lookups",
            BenchKind::Tls => "handshakes",
            BenchKind::Redis | BenchKind::Memcache => "ops",
            BenchKind::Postgres | BenchKind::Mysql => "txns",
        }
    }
}

/// Atomic counters updated by every worker on every request.
/// Used for live progress and the final summary.
#[derive(Debug, Default)]
pub struct LiveStats {
    pub requests: AtomicU64,
    pub errors: AtomicU64,
    pub bytes: AtomicU64,
    pub status_1xx: AtomicU64,
    pub status_2xx: AtomicU64,
    pub status_3xx: AtomicU64,
    pub status_4xx: AtomicU64,
    pub status_5xx: AtomicU64,

    // Phase timing accumulators (sum of microseconds + sample count) so the
    // live UI can show running averages without locking a histogram.
    pub dns_us_sum: AtomicU64,
    pub dns_count: AtomicU64,
    pub tcp_us_sum: AtomicU64,
    pub tcp_count: AtomicU64,
    pub tls_us_sum: AtomicU64,
    pub tls_count: AtomicU64,
    pub hs_us_sum: AtomicU64,
    pub hs_count: AtomicU64,
    pub ttfb_us_sum: AtomicU64,
    pub ttfb_count: AtomicU64,
    pub body_us_sum: AtomicU64,
    pub body_count: AtomicU64,
    pub connections: AtomicU64,
}

impl LiveStats {
    pub fn snapshot(&self) -> LiveSnapshot {
        LiveSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            status_1xx: self.status_1xx.load(Ordering::Relaxed),
            status_2xx: self.status_2xx.load(Ordering::Relaxed),
            status_3xx: self.status_3xx.load(Ordering::Relaxed),
            status_4xx: self.status_4xx.load(Ordering::Relaxed),
            status_5xx: self.status_5xx.load(Ordering::Relaxed),
            dns_us_sum: self.dns_us_sum.load(Ordering::Relaxed),
            dns_count: self.dns_count.load(Ordering::Relaxed),
            tcp_us_sum: self.tcp_us_sum.load(Ordering::Relaxed),
            tcp_count: self.tcp_count.load(Ordering::Relaxed),
            tls_us_sum: self.tls_us_sum.load(Ordering::Relaxed),
            tls_count: self.tls_count.load(Ordering::Relaxed),
            hs_us_sum: self.hs_us_sum.load(Ordering::Relaxed),
            hs_count: self.hs_count.load(Ordering::Relaxed),
            ttfb_us_sum: self.ttfb_us_sum.load(Ordering::Relaxed),
            ttfb_count: self.ttfb_count.load(Ordering::Relaxed),
            body_us_sum: self.body_us_sum.load(Ordering::Relaxed),
            body_count: self.body_count.load(Ordering::Relaxed),
            connections: self.connections.load(Ordering::Relaxed),
        }
    }

    pub fn record_connection_phases(&self, m: &PhaseConnect) {
        self.dns_us_sum.fetch_add(m.dns_us, Ordering::Relaxed);
        self.dns_count.fetch_add(1, Ordering::Relaxed);
        self.tcp_us_sum.fetch_add(m.tcp_us, Ordering::Relaxed);
        self.tcp_count.fetch_add(1, Ordering::Relaxed);
        if let Some(tls_us) = m.tls_us {
            self.tls_us_sum.fetch_add(tls_us, Ordering::Relaxed);
            self.tls_count.fetch_add(1, Ordering::Relaxed);
        }
        self.hs_us_sum.fetch_add(m.hs_us, Ordering::Relaxed);
        self.hs_count.fetch_add(1, Ordering::Relaxed);
        self.connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_request_phases(&self, ttfb_us: u64, body_us: u64) {
        self.ttfb_us_sum.fetch_add(ttfb_us, Ordering::Relaxed);
        self.ttfb_count.fetch_add(1, Ordering::Relaxed);
        self.body_us_sum.fetch_add(body_us, Ordering::Relaxed);
        self.body_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_phase_live(&self, phase: PhaseKind, us: u64) {
        let (sum, count) = match phase {
            PhaseKind::Dns => (&self.dns_us_sum, &self.dns_count),
            PhaseKind::Tcp => (&self.tcp_us_sum, &self.tcp_count),
            PhaseKind::Tls => (&self.tls_us_sum, &self.tls_count),
            PhaseKind::HttpHandshake => (&self.hs_us_sum, &self.hs_count),
        };
        sum.fetch_add(us, Ordering::Relaxed);
        count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_status(&self, status: u16) {
        let counter = match status / 100 {
            1 => &self.status_1xx,
            2 => &self.status_2xx,
            3 => &self.status_3xx,
            4 => &self.status_4xx,
            5 => &self.status_5xx,
            _ => return,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LiveSnapshot {
    pub requests: u64,
    pub errors: u64,
    pub bytes: u64,
    pub status_1xx: u64,
    pub status_2xx: u64,
    pub status_3xx: u64,
    pub status_4xx: u64,
    pub status_5xx: u64,

    pub dns_us_sum: u64,
    pub dns_count: u64,
    pub tcp_us_sum: u64,
    pub tcp_count: u64,
    pub tls_us_sum: u64,
    pub tls_count: u64,
    pub hs_us_sum: u64,
    pub hs_count: u64,
    pub ttfb_us_sum: u64,
    pub ttfb_count: u64,
    pub body_us_sum: u64,
    pub body_count: u64,
    pub connections: u64,
}

impl LiveSnapshot {
    pub fn avg(sum: u64, count: u64) -> Option<u64> {
        if count == 0 {
            None
        } else {
            Some(sum / count)
        }
    }
}

/// Per-connection phase timings in microseconds, captured on connect.
#[derive(Debug, Clone, Copy)]
pub struct PhaseConnect {
    pub dns_us: u64,
    pub tcp_us: u64,
    pub tls_us: Option<u64>,
    pub hs_us: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum PhaseKind {
    Dns,
    Tcp,
    Tls,
    HttpHandshake,
}

/// Per-worker results, merged into a `FinalReport` at the end.
pub struct WorkerReport {
    pub total: Histogram<u64>,
    pub ttfb: Histogram<u64>,
    pub body_dl: Histogram<u64>,
    pub dns: Histogram<u64>,
    pub tcp: Histogram<u64>,
    pub tls: Histogram<u64>,
    pub http_handshake: Histogram<u64>,
    pub status_codes: HashMap<u16, u64>,
    pub errors: HashMap<String, u64>,
    /// Per-operation latency histograms keyed by op name (e.g. "GET",
    /// "SET", "tpcb-update"). Used by backends with multiple op types.
    pub per_op: HashMap<String, Histogram<u64>>,
    pub bytes: u64,
    pub requests: u64,
}

fn new_hist() -> Histogram<u64> {
    // Track 1 µs … 60 s with 3 significant digits.
    Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("hdr histogram bounds")
}

fn record_us(h: &mut Histogram<u64>, us: u64) {
    let v = us.max(1).min(60_000_000);
    let _ = h.record(v);
}

impl WorkerReport {
    pub fn new() -> Self {
        Self {
            total: new_hist(),
            ttfb: new_hist(),
            body_dl: new_hist(),
            dns: new_hist(),
            tcp: new_hist(),
            tls: new_hist(),
            http_handshake: new_hist(),
            status_codes: HashMap::new(),
            errors: HashMap::new(),
            per_op: HashMap::new(),
            bytes: 0,
            requests: 0,
        }
    }

    /// Record a successful op with its label, e.g. "GET" or "tpcb-update".
    /// Bumps both the per-op histogram and the global `total` histogram so
    /// the dashboard's primary latency line still aggregates everything.
    pub fn record_op(&mut self, op: &str, total_us: u64) {
        record_us(&mut self.total, total_us);
        let h = self
            .per_op
            .entry(op.to_string())
            .or_insert_with(new_hist);
        record_us(h, total_us);
        self.requests += 1;
    }

    pub fn record_request(
        &mut self,
        ttfb_us: u64,
        body_us: u64,
        total_us: u64,
        status: u16,
        body_bytes: u64,
    ) {
        record_us(&mut self.total, total_us);
        record_us(&mut self.ttfb, ttfb_us);
        record_us(&mut self.body_dl, body_us);
        *self.status_codes.entry(status).or_insert(0) += 1;
        self.bytes += body_bytes;
        self.requests += 1;
    }

    /// Generic per-probe success record. Used by ping/tcp/dns/tls modes.
    pub fn record_probe(&mut self, total_us: u64) {
        record_us(&mut self.total, total_us);
        self.requests += 1;
    }

    /// Records a single per-phase timing into the named histogram. Used by
    /// non-HTTP probes that have a single phase of interest.
    pub fn record_phase(&mut self, phase: PhaseKind, us: u64) {
        let h = match phase {
            PhaseKind::Dns => &mut self.dns,
            PhaseKind::Tcp => &mut self.tcp,
            PhaseKind::Tls => &mut self.tls,
            PhaseKind::HttpHandshake => &mut self.http_handshake,
        };
        record_us(h, us);
    }

    pub fn record_connection(&mut self, m: &PhaseConnect) {
        record_us(&mut self.dns, m.dns_us);
        record_us(&mut self.tcp, m.tcp_us);
        if let Some(tls_us) = m.tls_us {
            record_us(&mut self.tls, tls_us);
        }
        record_us(&mut self.http_handshake, m.hs_us);
    }

    pub fn record_error(&mut self, live: &LiveStats, kind: &str) {
        *self.errors.entry(kind.to_string()).or_insert(0) += 1;
        self.requests += 1;
        live.errors.fetch_add(1, Ordering::Relaxed);
        live.requests.fetch_add(1, Ordering::Relaxed);
    }
}

pub struct FinalReport {
    pub kind: BenchKind,
    pub total_duration: Duration,
    pub total: Histogram<u64>,
    pub ttfb: Histogram<u64>,
    pub body_dl: Histogram<u64>,
    pub dns: Histogram<u64>,
    pub tcp: Histogram<u64>,
    pub tls: Histogram<u64>,
    pub http_handshake: Histogram<u64>,
    pub status_codes: HashMap<u16, u64>,
    pub errors: HashMap<String, u64>,
    pub per_op: HashMap<String, Histogram<u64>>,
    pub bytes: u64,
    pub requests: u64,
    pub success: u64,
    pub connections: usize,
    pub protocol: &'static str,
}

impl FinalReport {
    pub fn from_workers(
        kind: BenchKind,
        reports: Vec<WorkerReport>,
        total_duration: Duration,
        connections: usize,
        protocol: &'static str,
    ) -> Self {
        let mut total = new_hist();
        let mut ttfb = new_hist();
        let mut body_dl = new_hist();
        let mut dns = new_hist();
        let mut tcp = new_hist();
        let mut tls = new_hist();
        let mut http_handshake = new_hist();
        let mut status_codes: HashMap<u16, u64> = HashMap::new();
        let mut errors: HashMap<String, u64> = HashMap::new();
        let mut per_op: HashMap<String, Histogram<u64>> = HashMap::new();
        let mut bytes = 0u64;
        let mut requests = 0u64;
        let mut success = 0u64;

        for r in reports {
            total.add(&r.total).ok();
            ttfb.add(&r.ttfb).ok();
            body_dl.add(&r.body_dl).ok();
            dns.add(&r.dns).ok();
            tcp.add(&r.tcp).ok();
            tls.add(&r.tls).ok();
            http_handshake.add(&r.http_handshake).ok();
            for (k, v) in r.status_codes {
                *status_codes.entry(k).or_insert(0) += v;
                success += v;
            }
            for (k, v) in r.errors {
                *errors.entry(k).or_insert(0) += v;
            }
            for (op, hist) in r.per_op {
                let dst = per_op.entry(op).or_insert_with(new_hist);
                dst.add(&hist).ok();
            }
            bytes += r.bytes;
            requests += r.requests;
        }

        // For non-HTTP modes there are no status codes, so derive `success`
        // from total minus failures.
        let success = if status_codes.is_empty() {
            let err_total: u64 = errors.values().sum();
            requests.saturating_sub(err_total)
        } else {
            success
        };

        Self {
            kind,
            total_duration,
            total,
            ttfb,
            body_dl,
            dns,
            tcp,
            tls,
            http_handshake,
            status_codes,
            errors,
            per_op,
            bytes,
            requests,
            success,
            connections,
            protocol,
        }
    }
}

fn fmt_duration_us(us: u64) -> String {
    let s = us as f64 / 1_000_000.0;
    if s >= 1.0 {
        format!("{:>8.3} s", s)
    } else if s >= 0.001 {
        format!("{:>8.3} ms", s * 1_000.0)
    } else {
        format!("{:>8.3} µs", s * 1_000_000.0)
    }
}

fn fmt_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{:.2} {}", v, UNITS[i])
}

pub fn render_final(r: &FinalReport) -> String {
    let mut out = String::new();
    let total_secs = r.total_duration.as_secs_f64();
    let rps = r.requests as f64 / total_secs.max(f64::EPSILON);
    let throughput = r.bytes as f64 / total_secs.max(f64::EPSILON);

    out.push_str("\nSummary:\n");
    out.push_str(&format!("  Mode:           {}\n", r.kind.label()));
    out.push_str(&format!("  Protocol:       {}\n", r.protocol));
    out.push_str(&format!("  Workers:        {}\n", r.connections));
    out.push_str(&format!("  Total time:     {:.4} s\n", total_secs));
    let unit_label = format!("Total {}:", r.kind.unit_plural());
    out.push_str(&format!("  {:<18}{}\n", unit_label, r.requests));
    out.push_str(&format!("  Successful:     {}\n", r.success));
    let total_errors: u64 = r.errors.values().sum();
    out.push_str(&format!("  Errors:         {}\n", total_errors));
    if matches!(r.kind, BenchKind::Ping) && r.requests > 0 {
        let loss_pct = total_errors as f64 / r.requests as f64 * 100.0;
        out.push_str(&format!("  Packet loss:    {:.2}%\n", loss_pct));
    }
    let rate_label = format!("{}/sec:", r.kind.unit_plural());
    out.push_str(&format!("  {:<18}{:.2}\n", rate_label, rps));
    if r.bytes > 0 {
        out.push_str(&format!(
            "  Throughput:     {}/s\n",
            fmt_bytes(throughput as u64)
        ));
        out.push_str(&format!("  Total data:     {}\n", fmt_bytes(r.bytes)));
        if r.success > 0 {
            out.push_str(&format!(
                "  Size/request:   {}\n",
                fmt_bytes(r.bytes / r.success.max(1))
            ));
        }
    }

    // Phase tables — only render the rows that actually applied for this
    // bench kind so DNS/TLS rows don't show "—" for an HTTP-less probe.
    let mut conn_rows: Vec<(&str, &Histogram<u64>)> = Vec::new();
    let mut req_rows: Vec<(&str, &Histogram<u64>)> = Vec::new();
    match r.kind {
        BenchKind::Http => {
            conn_rows.push(("DNS lookup    ", &r.dns));
            conn_rows.push(("TCP connect   ", &r.tcp));
            conn_rows.push(("TLS handshake ", &r.tls));
            conn_rows.push(("HTTP handshake", &r.http_handshake));
            req_rows.push(("Time to first byte", &r.ttfb));
            req_rows.push(("Body download     ", &r.body_dl));
            req_rows.push(("Total             ", &r.total));
        }
        BenchKind::Tls => {
            conn_rows.push(("DNS lookup    ", &r.dns));
            conn_rows.push(("TCP connect   ", &r.tcp));
            conn_rows.push(("TLS handshake ", &r.tls));
            req_rows.push(("Total             ", &r.total));
        }
        BenchKind::Tcp => {
            conn_rows.push(("DNS lookup    ", &r.dns));
            conn_rows.push(("TCP connect   ", &r.tcp));
            req_rows.push(("Total             ", &r.total));
        }
        BenchKind::Dns => {
            conn_rows.push(("DNS lookup    ", &r.dns));
            req_rows.push(("Total             ", &r.total));
        }
        BenchKind::Ping => {
            req_rows.push(("Round-trip time   ", &r.total));
        }
        BenchKind::Redis | BenchKind::Memcache => {
            req_rows.push(("Op latency        ", &r.total));
        }
        BenchKind::Postgres | BenchKind::Mysql => {
            req_rows.push(("Transaction time  ", &r.total));
        }
    }
    if !conn_rows.is_empty() {
        out.push_str("\nConnection phases (per connection):\n");
        out.push_str(&phase_table(&conn_rows));
    }
    if !req_rows.is_empty() {
        let header = match r.kind {
            BenchKind::Ping => "\nPer-probe timings:\n",
            BenchKind::Http => "\nRequest phases (per request):\n",
            _ => "\nPer-probe timings:\n",
        };
        out.push_str(header);
        out.push_str(&phase_table(&req_rows));
    }

    if !r.per_op.is_empty() {
        out.push_str("\nPer-operation latency:\n");
        let mut ops: Vec<(&String, &Histogram<u64>)> = r.per_op.iter().collect();
        // Sort by total count descending so heaviest ops list first.
        ops.sort_by(|a, b| b.1.len().cmp(&a.1.len()));
        let rows: Vec<(&str, &Histogram<u64>)> = ops
            .iter()
            .map(|(name, h)| (name.as_str(), *h))
            .collect();
        // Reuse the existing aligned phase_table renderer.
        let labelled: Vec<(&str, &Histogram<u64>)> = rows;
        out.push_str(&format!(
            "  {:<22} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}\n",
            "op", "count", "min", "mean", "p50", "p95", "p99", "max"
        ));
        for (name, h) in &labelled {
            out.push_str(&format!(
                "  {:<22} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}\n",
                name,
                h.len(),
                short_dur(h.min()),
                short_dur(h.mean() as u64),
                short_dur(h.value_at_quantile(0.50)),
                short_dur(h.value_at_quantile(0.95)),
                short_dur(h.value_at_quantile(0.99)),
                short_dur(h.max()),
            ));
        }
    }

    if r.total.len() > 0 {
        out.push_str("\nLatency distribution:\n");
        for &p in &[10.0, 25.0, 50.0, 75.0, 90.0, 95.0, 99.0, 99.9, 99.99] {
            let v = r.total.value_at_quantile(p / 100.0);
            out.push_str(&format!("  {:>6}%: {}\n", format_pct(p), fmt_duration_us(v)));
        }

        out.push_str(&format!(
            "\nLatency histogram:\n{}",
            render_histogram(&r.total)
        ));
    }

    if !r.status_codes.is_empty() {
        out.push_str("\nStatus codes:\n");
        let mut codes: Vec<(u16, u64)> = r.status_codes.iter().map(|(&k, &v)| (k, v)).collect();
        codes.sort_by_key(|(k, _)| *k);
        for (code, count) in codes {
            let pct = count as f64 / r.requests.max(1) as f64 * 100.0;
            out.push_str(&format!("  [{}] {} ({:.2}%)\n", code, count, pct));
        }
    }

    if !r.errors.is_empty() {
        out.push_str("\nErrors:\n");
        let mut errs: Vec<(&String, &u64)> = r.errors.iter().collect();
        errs.sort_by(|a, b| b.1.cmp(a.1));
        for (kind, count) in errs {
            out.push_str(&format!("  {} × {}\n", count, kind));
        }
    }

    out
}

fn format_pct(p: f64) -> String {
    if p.fract() == 0.0 {
        format!("{}", p as u32)
    } else {
        format!("{}", p)
    }
}

fn phase_table(rows: &[(&str, &Histogram<u64>)]) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "  {:<22} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}\n",
        "phase", "min", "mean", "p50", "p95", "p99", "max"
    ));
    for (label, h) in rows {
        if h.len() == 0 {
            out.push_str(&format!(
                "  {:<22} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}\n",
                label.trim(),
                "—",
                "—",
                "—",
                "—",
                "—",
                "—",
            ));
            continue;
        }
        out.push_str(&format!(
            "  {:<22} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}\n",
            label.trim(),
            short_dur(h.min()),
            short_dur(h.mean() as u64),
            short_dur(h.value_at_quantile(0.50)),
            short_dur(h.value_at_quantile(0.95)),
            short_dur(h.value_at_quantile(0.99)),
            short_dur(h.max()),
        ));
    }
    out
}

fn short_dur(us: u64) -> String {
    let s = us as f64 / 1_000_000.0;
    if s >= 1.0 {
        format!("{:.2} s", s)
    } else if s >= 0.001 {
        format!("{:.2} ms", s * 1_000.0)
    } else {
        format!("{:.0} µs", s * 1_000_000.0)
    }
}

fn render_histogram(h: &Histogram<u64>) -> String {
    if h.len() == 0 {
        return String::new();
    }
    // Compress to ~10 bins between p1 and p99 for readability
    // (extreme outliers can otherwise flatten the chart to a single bar).
    let lo = h.value_at_quantile(0.01).max(h.min());
    let hi = h.value_at_quantile(0.99).max(lo + 1);
    let bins = 10u64;
    let step = ((hi - lo) / bins).max(1);

    let mut buckets: Vec<u64> = vec![0; bins as usize];
    for v in h.iter_recorded() {
        let val = v.value_iterated_to();
        if val < lo {
            buckets[0] += v.count_since_last_iteration();
            continue;
        }
        let idx = ((val - lo) / step).min(bins - 1) as usize;
        buckets[idx] += v.count_since_last_iteration();
    }

    let max_count = *buckets.iter().max().unwrap_or(&1);
    let bar_width = 40usize;
    let mut out = String::new();
    for (i, &count) in buckets.iter().enumerate() {
        let edge = lo + (i as u64) * step;
        let bar_len = ((count as f64 / max_count as f64) * bar_width as f64) as usize;
        let bar: String = "■".repeat(bar_len);
        out.push_str(&format!(
            "  {} [{:>8}] |{}\n",
            fmt_duration_us(edge),
            count,
            bar
        ));
    }
    out
}
