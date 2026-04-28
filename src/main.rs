use std::collections::VecDeque;
use std::io::{stdout, IsTerminal, Write};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use http::{HeaderName, HeaderValue, Uri};
use parking_lot::Mutex;
use ratatui::crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::symbols::Marker;
use ratatui::widgets::{Axis, Block, Borders, Chart, Dataset, Gauge, GraphType, Paragraph};
use ratatui::{backend::CrosstermBackend, Terminal};
use tokio::task::JoinHandle;

mod stats;
mod tls;
mod worker;

use stats::{render_final, FinalReport, LiveSnapshot, LiveStats, WorkerReport};
use worker::{run_worker, Scheme, Target, WorkerConfig, WorkerHandles};

#[derive(Parser, Debug, Clone)]
#[command(
    name = "httpbenchy",
    version,
    about = "Ultra-fast HTTP(S) benchmark with a live ratatui dashboard",
    long_about = None
)]
struct Cli {
    /// Target URL (http:// or https://)
    url: String,

    /// Number of concurrent connections.
    #[arg(short = 'c', long = "connections", default_value_t = 50)]
    connections: usize,

    /// Total number of requests (overrides --duration).
    #[arg(short = 'n', long = "requests")]
    requests: Option<u64>,

    /// Benchmark duration, e.g. 10s, 1m, 30s. Defaults to 10s if neither -n nor -z is given.
    #[arg(short = 'z', long = "duration", value_parser = humantime::parse_duration)]
    duration: Option<Duration>,

    /// HTTP method.
    #[arg(short = 'm', long = "method", default_value = "GET")]
    method: String,

    /// Request header (repeatable): -H "Name: Value".
    #[arg(short = 'H', long = "header")]
    headers: Vec<String>,

    /// Request body as a literal string.
    #[arg(short = 'd', long = "body")]
    body: Option<String>,

    /// Per-request timeout (connect + send + body).
    #[arg(long = "timeout", value_parser = humantime::parse_duration, default_value = "30s")]
    timeout: Duration,

    /// Skip TLS certificate verification (DANGEROUS — testing only).
    #[arg(short = 'k', long = "insecure")]
    insecure: bool,

    /// Force HTTP/2 (h2c for plaintext, ALPN-first for TLS).
    #[arg(long = "http2")]
    http2: bool,

    /// Disable HTTP keep-alive (one request per connection).
    #[arg(long = "no-keepalive")]
    no_keepalive: bool,

    /// Tokio worker threads (defaults to number of CPUs).
    #[arg(short = 't', long = "threads")]
    threads: Option<usize>,

    /// Disable the live ratatui dashboard (still prints final report).
    #[arg(long = "no-tui")]
    no_tui: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    tls::install_default_provider();

    let threads = cli.threads.unwrap_or_else(num_cpus::get).max(1);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?;

    runtime.block_on(async_main(cli, threads))
}

async fn async_main(cli: Cli, threads: usize) -> Result<()> {
    let target = parse_target(&cli.url)?;
    let method: http::Method = cli
        .method
        .parse()
        .with_context(|| format!("invalid HTTP method: {}", cli.method))?;
    let headers = parse_headers(&cli.headers)?;
    let body = cli.body.clone().unwrap_or_default().into_bytes().into();

    if cli.requests.is_some() && cli.duration.is_some() {
        eprintln!("note: --requests overrides --duration");
    }
    let (duration, request_budget) = match (cli.requests, cli.duration) {
        (Some(n), _) => (None, n as i64),
        (None, Some(d)) => (Some(d), -1i64),
        (None, None) => (Some(Duration::from_secs(10)), -1i64),
    };

    let cfg = Arc::new(WorkerConfig {
        target: Arc::new(target),
        method,
        headers: Arc::new(headers),
        body,
        timeout: cli.timeout,
        keepalive: !cli.no_keepalive,
        force_h2: cli.http2,
        insecure: cli.insecure,
    });

    let live = Arc::new(LiveStats::default());
    let stop = Arc::new(AtomicBool::new(false));
    let remaining = Arc::new(AtomicI64::new(request_budget));
    let proto_observed = Arc::new(Mutex::new(None::<&'static str>));

    // Ctrl+C handler.
    {
        let stop = stop.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            stop.store(true, Ordering::Relaxed);
        });
    }

    // Duration deadline.
    if let Some(dur) = duration {
        let stop = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(dur).await;
            stop.store(true, Ordering::Relaxed);
        });
    }

    let started_at = Instant::now();

    // Spawn workers.
    let mut handles: Vec<JoinHandle<WorkerReport>> = Vec::with_capacity(cli.connections);
    for _ in 0..cli.connections {
        let cfg = cfg.clone();
        let h = WorkerHandles {
            stop: stop.clone(),
            remaining: remaining.clone(),
            live: live.clone(),
        };
        let proto = proto_observed.clone();
        handles.push(tokio::spawn(async move { run_worker(cfg, h, proto).await }));
    }

    // Live UI.
    let use_tui = !cli.no_tui && stdout().is_terminal();
    if use_tui {
        run_tui(
            &cli,
            &cfg,
            threads,
            started_at,
            duration,
            cli.requests,
            live.clone(),
            stop.clone(),
            proto_observed.clone(),
            &handles,
        )
        .await?;
    } else {
        run_plain(
            &cli,
            started_at,
            duration,
            cli.requests,
            live.clone(),
            stop.clone(),
            &handles,
        )
        .await;
    }

    // Drain results (workers should be at-or-near completion at this point).
    stop.store(true, Ordering::Relaxed);
    let mut reports = Vec::with_capacity(handles.len());
    for h in handles {
        match h.await {
            Ok(r) => reports.push(r),
            Err(_) => {}
        }
    }

    let total_duration = started_at.elapsed();
    let protocol = proto_observed.lock().unwrap_or("HTTP/1.1");
    let final_report = FinalReport::from_workers(reports, total_duration, cli.connections, protocol);

    print!("{}", render_final(&final_report));
    stdout().flush().ok();
    Ok(())
}

fn parse_target(input: &str) -> Result<Target> {
    let parsed = url::Url::parse(input).with_context(|| format!("invalid URL: {input}"))?;
    let scheme = match parsed.scheme() {
        "http" => Scheme::Http,
        "https" => Scheme::Https,
        s => bail!("unsupported scheme: {s}"),
    };
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("URL must have a host"))?
        .to_string();
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| anyhow!("unable to determine port"))?;
    let authority = match (scheme, port) {
        (Scheme::Http, 80) | (Scheme::Https, 443) => host.clone(),
        _ => format!("{}:{}", host, port),
    };
    let path = if parsed.path().is_empty() {
        "/".to_string()
    } else {
        parsed.path().to_string()
    };
    let path_with_query = match parsed.query() {
        Some(q) if !q.is_empty() => format!("{path}?{q}"),
        _ => path,
    };
    let path_uri: Uri = path_with_query.parse().context("path uri")?;
    let full = format!("{}://{}{}", parsed.scheme(), authority, path_uri.path_and_query().map(|p| p.as_str()).unwrap_or(""));
    let full_uri: Uri = full.parse().context("full uri")?;

    Ok(Target {
        scheme,
        host,
        port,
        authority,
        uri: full_uri,
        path: path_uri,
    })
}

fn parse_headers(raw: &[String]) -> Result<Vec<(HeaderName, HeaderValue)>> {
    let mut out = Vec::with_capacity(raw.len());
    for h in raw {
        let (name, value) = h
            .split_once(':')
            .ok_or_else(|| anyhow!("invalid header (expected 'Name: Value'): {h}"))?;
        let name: HeaderName = name
            .trim()
            .parse()
            .with_context(|| format!("invalid header name: {name}"))?;
        let value: HeaderValue = value
            .trim()
            .parse()
            .with_context(|| format!("invalid header value: {value}"))?;
        out.push((name, value));
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
async fn run_tui(
    cli: &Cli,
    cfg: &Arc<WorkerConfig>,
    threads: usize,
    started_at: Instant,
    duration: Option<Duration>,
    request_target: Option<u64>,
    live: Arc<LiveStats>,
    stop: Arc<AtomicBool>,
    proto: Arc<Mutex<Option<&'static str>>>,
    handles: &[JoinHandle<WorkerReport>],
) -> Result<()> {
    enable_raw_mode()?;
    let mut out = stdout();
    execute!(out, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(out);
    let mut terminal = Terminal::new(backend)?;

    let mut last_snapshot = LiveSnapshot::default();
    let mut last_tick = Instant::now();
    let history_len = 240usize;
    let mut rps_history: VecDeque<u64> = VecDeque::with_capacity(history_len);
    rps_history.extend(std::iter::repeat(0u64).take(history_len));
    let mut last_progress_at = Instant::now();

    let result = tui_loop(
        &mut terminal,
        cli,
        cfg,
        threads,
        started_at,
        duration,
        request_target,
        &live,
        &stop,
        &proto,
        handles,
        &mut last_snapshot,
        &mut last_tick,
        &mut rps_history,
        &mut last_progress_at,
    )
    .await;

    // Always restore the terminal even if drawing failed.
    let mut out = stdout();
    let _ = execute!(out, LeaveAlternateScreen);
    let _ = disable_raw_mode();

    result
}

#[allow(clippy::too_many_arguments)]
async fn tui_loop(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    cli: &Cli,
    cfg: &Arc<WorkerConfig>,
    threads: usize,
    started_at: Instant,
    duration: Option<Duration>,
    request_target: Option<u64>,
    live: &Arc<LiveStats>,
    stop: &Arc<AtomicBool>,
    proto: &Arc<Mutex<Option<&'static str>>>,
    handles: &[JoinHandle<WorkerReport>],
    last_snapshot: &mut LiveSnapshot,
    last_tick: &mut Instant,
    rps_history: &mut VecDeque<u64>,
    last_progress_at: &mut Instant,
) -> Result<()> {
    let stall_grace = Duration::from_secs(2);
    let stall_threshold = Duration::from_secs(2);

    loop {
        // Update RPS / bytes-per-second from atomic deltas.
        let now = Instant::now();
        let elapsed_since_tick = now.duration_since(*last_tick).as_secs_f64().max(1e-3);
        let snap = live.snapshot();
        let req_delta = snap.requests.saturating_sub(last_snapshot.requests);
        let byte_delta = snap.bytes.saturating_sub(last_snapshot.bytes);
        let current_rps = req_delta as f64 / elapsed_since_tick;
        let current_bps = byte_delta as f64 / elapsed_since_tick;
        rps_history.push_back(current_rps as u64);
        if rps_history.len() > 240 {
            rps_history.pop_front();
        }
        *last_snapshot = snap;
        *last_tick = now;

        if req_delta > 0 {
            *last_progress_at = now;
        }
        let elapsed = started_at.elapsed();
        let stalled_for = now.duration_since(*last_progress_at);
        let stalled = elapsed > stall_grace && stalled_for > stall_threshold;

        let proto_str = proto.lock().unwrap_or("negotiating…");

        terminal.draw(|frame| {
            draw_dashboard(
                frame,
                cli,
                cfg,
                threads,
                proto_str,
                elapsed,
                duration,
                request_target,
                snap,
                current_rps,
                current_bps,
                rps_history,
                stalled,
                stalled_for,
            );
        })?;

        // Pump keyboard at up to 100 ms.
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    let ctrl_c = key.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('C'));
                    if ctrl_c
                        || matches!(key.code, KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc)
                    {
                        stop.store(true, Ordering::Relaxed);
                    }
                }
            }
        }

        if handles.iter().all(|h| h.is_finished()) && stop.load(Ordering::Relaxed) {
            break;
        }
        if handles.iter().all(|h| h.is_finished()) {
            // Workers finished without stop being set (request count reached).
            break;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn draw_dashboard(
    frame: &mut ratatui::Frame<'_>,
    cli: &Cli,
    cfg: &Arc<WorkerConfig>,
    threads: usize,
    proto: &'static str,
    elapsed: Duration,
    duration: Option<Duration>,
    request_target: Option<u64>,
    snap: LiveSnapshot,
    current_rps: f64,
    current_bps: f64,
    rps_history: &VecDeque<u64>,
    stalled: bool,
    stalled_for: Duration,
) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),  // header
            Constraint::Length(3),  // progress
            Constraint::Length(8),  // body (live + phases + responses)
            Constraint::Min(10),    // chart (req/s over time)
            Constraint::Length(1),  // footer
        ])
        .split(area);

    draw_header(frame, chunks[0], cli, cfg, threads, proto);
    draw_progress(
        frame,
        chunks[1],
        elapsed,
        duration,
        request_target,
        snap.requests,
        stalled,
        stalled_for,
    );
    draw_body(frame, chunks[2], snap, current_rps, current_bps);
    draw_chart(frame, chunks[3], rps_history, current_rps, stalled);
    draw_footer(frame, chunks[4]);
}

fn draw_header(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    cli: &Cli,
    cfg: &Arc<WorkerConfig>,
    threads: usize,
    proto: &'static str,
) {
    let title = format!(" httpbenchy v{} ", env!("CARGO_PKG_VERSION"));
    let block = Block::default()
        .borders(Borders::ALL)
        .title(Span::styled(
            title,
            Style::default().add_modifier(Modifier::BOLD).fg(Color::Cyan),
        ));
    let line1 = Line::from(vec![
        Span::styled("Target  ", Style::default().fg(Color::DarkGray)),
        Span::styled(cli.url.clone(), Style::default().fg(Color::White)),
    ]);
    let line2 = Line::from(vec![
        Span::styled("Method ", Style::default().fg(Color::DarkGray)),
        Span::raw(cfg.method.as_str().to_string()),
        Span::raw("   "),
        Span::styled("Proto ", Style::default().fg(Color::DarkGray)),
        Span::styled(proto, Style::default().fg(Color::Yellow)),
        Span::raw("   "),
        Span::styled("Conns ", Style::default().fg(Color::DarkGray)),
        Span::raw(cli.connections.to_string()),
        Span::raw("   "),
        Span::styled("Threads ", Style::default().fg(Color::DarkGray)),
        Span::raw(threads.to_string()),
        Span::raw("   "),
        Span::styled("Keep-alive ", Style::default().fg(Color::DarkGray)),
        Span::raw(if cfg.keepalive { "on" } else { "off" }),
    ]);
    let p = Paragraph::new(vec![line1, line2]).block(block);
    frame.render_widget(p, area);
}

#[allow(clippy::too_many_arguments)]
fn draw_progress(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    elapsed: Duration,
    duration: Option<Duration>,
    request_target: Option<u64>,
    requests: u64,
    stalled: bool,
    stalled_for: Duration,
) {
    let (mut label, ratio) = match (duration, request_target) {
        (_, Some(n)) => {
            let r = (requests as f64 / n.max(1) as f64).clamp(0.0, 1.0);
            (format!("{}/{}  reqs", fmt_int(requests), fmt_int(n)), r)
        }
        (Some(d), _) => {
            let r = (elapsed.as_secs_f64() / d.as_secs_f64().max(1e-9)).clamp(0.0, 1.0);
            (format!("{} / {}", fmt_clock(elapsed), fmt_clock(d)), r)
        }
        _ => (fmt_clock(elapsed), 0.0),
    };

    let (gauge_color, title) = if stalled {
        label.push_str(&format!(
            "   ⚠ STALLED for {}s — no completed requests",
            stalled_for.as_secs()
        ));
        (Color::Red, " progress (stalled) ")
    } else {
        (Color::Green, " progress ")
    };

    let gauge = Gauge::default()
        .block(Block::default().borders(Borders::ALL).title(title))
        .gauge_style(
            Style::default()
                .fg(gauge_color)
                .bg(Color::Reset)
                .add_modifier(Modifier::BOLD),
        )
        .ratio(ratio)
        .label(label);
    frame.render_widget(gauge, area);
}

fn draw_body(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    snap: LiveSnapshot,
    rps: f64,
    bps: f64,
) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(38),
            Constraint::Percentage(34),
            Constraint::Percentage(28),
        ])
        .split(area);

    // Live counters
    let rps_color = if rps > 10_000.0 {
        Color::Green
    } else if rps > 1_000.0 {
        Color::Yellow
    } else {
        Color::Red
    };
    let lines = vec![
        big_kv("Reqs/sec   ", format!("{:>14.1}", rps), rps_color),
        big_kv("Bytes/sec  ", format!("{:>14}", fmt_bytes_rate(bps)), Color::Cyan),
        big_kv("Total reqs ", format!("{:>14}", fmt_int(snap.requests)), Color::White),
        big_kv("Total data ", format!("{:>14}", fmt_bytes(snap.bytes)), Color::White),
        big_kv("Conns made ", format!("{:>14}", fmt_int(snap.connections)), Color::White),
        big_kv(
            "Errors     ",
            format!("{:>14}", fmt_int(snap.errors)),
            if snap.errors > 0 {
                Color::Red
            } else {
                Color::DarkGray
            },
        ),
    ];
    let live = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(" live "));
    frame.render_widget(live, cols[0]);

    // Phase averages (per connection / per request).
    let dns = LiveSnapshot::avg(snap.dns_us_sum, snap.dns_count);
    let tcp = LiveSnapshot::avg(snap.tcp_us_sum, snap.tcp_count);
    let tls = LiveSnapshot::avg(snap.tls_us_sum, snap.tls_count);
    let hs = LiveSnapshot::avg(snap.hs_us_sum, snap.hs_count);
    let ttfb = LiveSnapshot::avg(snap.ttfb_us_sum, snap.ttfb_count);
    let body = LiveSnapshot::avg(snap.body_us_sum, snap.body_count);

    let phase_lines = vec![
        phase_kv("DNS lookup    ", dns, Color::Magenta),
        phase_kv("TCP connect   ", tcp, Color::Blue),
        phase_kv("TLS handshake ", tls, Color::Cyan),
        phase_kv("HTTP handshake", hs, Color::Gray),
        phase_kv("TTFB          ", ttfb, Color::Yellow),
        phase_kv("Body download ", body, Color::Green),
    ];
    let phases = Paragraph::new(phase_lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" phase averages "),
    );
    frame.render_widget(phases, cols[1]);

    // Status code distribution
    let total = snap.requests.max(1);
    let pct = |n: u64| n as f64 / total as f64 * 100.0;
    let status_lines = vec![
        status_line("1xx", snap.status_1xx, pct(snap.status_1xx), Color::Gray),
        status_line("2xx", snap.status_2xx, pct(snap.status_2xx), Color::Green),
        status_line("3xx", snap.status_3xx, pct(snap.status_3xx), Color::Cyan),
        status_line("4xx", snap.status_4xx, pct(snap.status_4xx), Color::Yellow),
        status_line("5xx", snap.status_5xx, pct(snap.status_5xx), Color::Red),
        status_line("err", snap.errors, pct(snap.errors), Color::Magenta),
    ];
    let status = Paragraph::new(status_lines)
        .block(Block::default().borders(Borders::ALL).title(" responses "));
    frame.render_widget(status, cols[2]);
}

fn phase_kv(label: &str, us: Option<u64>, color: Color) -> Line<'static> {
    let value = match us {
        Some(v) => fmt_short_dur(v),
        None => "—".to_string(),
    };
    Line::from(vec![
        Span::styled(label.to_string(), Style::default().fg(Color::DarkGray)),
        Span::raw("  "),
        Span::styled(
            format!("{:>10}", value),
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ),
    ])
}

fn fmt_short_dur(us: u64) -> String {
    let s = us as f64 / 1_000_000.0;
    if s >= 1.0 {
        format!("{:.2} s", s)
    } else if s >= 0.001 {
        format!("{:.2} ms", s * 1_000.0)
    } else {
        format!("{:.0} µs", s * 1_000_000.0)
    }
}

fn status_line(label: &str, n: u64, pct: f64, color: Color) -> Line<'static> {
    let bar_width = 20usize;
    let filled = ((pct / 100.0) * bar_width as f64).round() as usize;
    let bar: String = "█".repeat(filled.min(bar_width))
        + &"·".repeat(bar_width.saturating_sub(filled.min(bar_width)));
    Line::from(vec![
        Span::styled(format!(" {} ", label), Style::default().fg(color).add_modifier(Modifier::BOLD)),
        Span::styled(bar, Style::default().fg(color)),
        Span::raw(format!(" {:>10}  {:>5.1}%", fmt_int(n), pct)),
    ])
}

fn big_kv(label: &str, value: String, value_color: Color) -> Line<'static> {
    Line::from(vec![
        Span::styled(label.to_string(), Style::default().fg(Color::DarkGray)),
        Span::styled(
            value,
            Style::default()
                .fg(value_color)
                .add_modifier(Modifier::BOLD),
        ),
    ])
}

fn draw_chart(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    rps_history: &VecDeque<u64>,
    current_rps: f64,
    stalled: bool,
) {
    let len = rps_history.len();
    let peak = rps_history.iter().copied().max().unwrap_or(0);
    let avg: f64 = if len > 0 {
        rps_history.iter().copied().sum::<u64>() as f64 / len as f64
    } else {
        0.0
    };
    // Round y-axis up to a "nice" ceiling so the chart doesn't bounce on
    // every tick.
    let y_max = nice_ceiling((peak as f64).max(current_rps).max(1.0));
    let data: Vec<(f64, f64)> = rps_history
        .iter()
        .enumerate()
        .map(|(i, v)| (i as f64, *v as f64))
        .collect();

    let line_color = if stalled { Color::Red } else { Color::Green };
    let datasets = vec![Dataset::default()
        .name("req/s")
        .marker(Marker::Braille)
        .graph_type(GraphType::Line)
        .style(Style::default().fg(line_color))
        .data(&data)];

    let title = format!(
        " req/s graph    now {:.0}    avg {:.0}    peak {} ",
        current_rps,
        avg,
        fmt_int(peak)
    );

    let chart = Chart::new(datasets)
        .block(Block::default().borders(Borders::ALL).title(title))
        .x_axis(
            Axis::default()
                .style(Style::default().fg(Color::DarkGray))
                .bounds([0.0, len.saturating_sub(1).max(1) as f64]),
        )
        .y_axis(
            Axis::default()
                .style(Style::default().fg(Color::DarkGray))
                .bounds([0.0, y_max])
                .labels(vec![
                    Span::raw("0"),
                    Span::raw(fmt_int((y_max / 2.0) as u64)),
                    Span::raw(fmt_int(y_max as u64)),
                ]),
        );
    frame.render_widget(chart, area);
}

fn nice_ceiling(v: f64) -> f64 {
    if v <= 0.0 {
        return 1.0;
    }
    let exp = v.log10().floor();
    let mag = 10f64.powf(exp);
    let n = (v / mag).ceil();
    let snap = if n <= 1.0 {
        1.0
    } else if n <= 2.0 {
        2.0
    } else if n <= 5.0 {
        5.0
    } else {
        10.0
    };
    snap * mag
}

fn draw_footer(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let p = Paragraph::new(Line::from(vec![
        Span::styled(" q ", Style::default().bg(Color::DarkGray).fg(Color::White)),
        Span::raw("/"),
        Span::styled(" Esc ", Style::default().bg(Color::DarkGray).fg(Color::White)),
        Span::raw("/"),
        Span::styled(" Ctrl-C ", Style::default().bg(Color::DarkGray).fg(Color::White)),
        Span::styled(
            "  stop and print summary",
            Style::default().fg(Color::DarkGray),
        ),
    ]));
    frame.render_widget(p, area);
}

async fn run_plain(
    cli: &Cli,
    started_at: Instant,
    duration: Option<Duration>,
    request_target: Option<u64>,
    live: Arc<LiveStats>,
    stop: Arc<AtomicBool>,
    handles: &[JoinHandle<WorkerReport>],
) {
    let _ = cli;
    let mut last_snap = LiveSnapshot::default();
    let mut last_tick = Instant::now();

    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let snap = live.snapshot();
        let now = Instant::now();
        let dt = now.duration_since(last_tick).as_secs_f64().max(1e-3);
        let rps = snap.requests.saturating_sub(last_snap.requests) as f64 / dt;
        last_snap = snap;
        last_tick = now;

        let elapsed = started_at.elapsed();
        let progress = match (duration, request_target) {
            (_, Some(n)) => format!("{}/{}", snap.requests, n),
            (Some(d), _) => format!("{}/{}", fmt_clock(elapsed), fmt_clock(d)),
            _ => fmt_clock(elapsed),
        };
        eprint!(
            "\r[{}] reqs={:<10} rps={:<10.1} bytes={:<10} errs={:<6}",
            progress,
            fmt_int(snap.requests),
            rps,
            fmt_bytes(snap.bytes),
            snap.errors,
        );
        let _ = std::io::stderr().flush();

        if handles.iter().all(|h| h.is_finished()) {
            eprintln!();
            break;
        }
        if stop.load(Ordering::Relaxed) && handles.iter().all(|h| h.is_finished()) {
            eprintln!();
            break;
        }
    }
}

fn fmt_int(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(*b as char);
    }
    out
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

fn fmt_bytes_rate(bps: f64) -> String {
    fmt_bytes(bps as u64) + "/s"
}

fn fmt_clock(d: Duration) -> String {
    let total = d.as_secs();
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    if h > 0 {
        format!("{:02}:{:02}:{:02}", h, m, s)
    } else {
        format!("{:02}:{:02}", m, s)
    }
}
