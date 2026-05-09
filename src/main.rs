use std::collections::VecDeque;
use std::io::{stdout, IsTerminal, Write};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use http::{HeaderName, HeaderValue, Uri};
use parking_lot::Mutex;
use ratatui::crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols::Marker;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Block, Borders, Chart, Dataset, Gauge, GraphType, Paragraph};
use ratatui::{backend::CrosstermBackend, Terminal};
use tokio::task::JoinHandle;

mod dns_bench;
mod memcache_bench;
mod mysql_bench;
mod ping_bench;
mod postgres_bench;
mod ratelimit;
mod redis_bench;
mod s3_bench;
mod stats;
mod tcp_bench;
mod tinyice_bench;
mod tls;
mod tls_bench;
mod worker;

use stats::{render_final, BenchKind, FinalReport, LiveSnapshot, LiveStats, WorkerReport};
use worker::{run_worker as run_http_worker, Scheme, Target, WorkerConfig, WorkerHandles};

#[derive(Parser, Debug)]
#[command(
    name = "databench",
    version,
    about = "Multi-protocol network benchmark — HTTP(S), ICMP ping, TCP, DNS, TLS",
    long_about = None
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,

    /// Number of concurrent workers (connections / probes in flight).
    #[arg(short = 'c', long = "connections", default_value_t = 50, global = true)]
    connections: usize,

    /// Total number of probes (overrides --duration).
    #[arg(short = 'n', long = "requests", global = true)]
    requests: Option<u64>,

    /// Benchmark duration, e.g. 10s, 1m. Defaults to 10s if neither -n nor -z is given.
    #[arg(short = 'z', long = "duration", value_parser = humantime::parse_duration, global = true)]
    duration: Option<Duration>,

    /// Per-probe timeout.
    #[arg(long = "timeout", value_parser = humantime::parse_duration, default_value = "30s", global = true)]
    timeout: Duration,

    /// Tokio worker threads (defaults to number of CPUs).
    #[arg(short = 't', long = "threads", global = true)]
    threads: Option<usize>,

    /// Disable the live ratatui dashboard (still prints the final report).
    #[arg(long = "no-tui", global = true)]
    no_tui: bool,

    /// Cap total throughput at this many probes per second (token-bucket).
    /// Applies across all workers. Useful for gentle production probing.
    #[arg(long = "qps", global = true)]
    qps: Option<f64>,

    /// Warmup duration. Probes during warmup hit the server but their
    /// latencies are discarded and they don't consume `--requests`. Use
    /// to skip cold-cache / JIT / prepared-statement-priming artifacts.
    /// Recommended: 10s for KV stores, 30s for SQL.
    #[arg(long = "warmup", value_parser = humantime::parse_duration, global = true)]
    warmup: Option<Duration>,
}

#[derive(Subcommand, Debug, Clone)]
enum Cmd {
    /// HTTP(S) benchmark.
    Http(HttpArgs),
    /// ICMP echo (ping) benchmark.
    Ping(PingArgs),
    /// TCP connect-time benchmark.
    Tcp(TcpArgs),
    /// DNS lookup benchmark (system resolver).
    Dns(DnsArgs),
    /// TLS handshake-only benchmark.
    Tls(TlsArgs),
    /// Redis benchmark (sandboxed by default; `--cmd` for custom).
    Redis(RedisArgs),
    /// PostgreSQL benchmark (TPC-B-lite by default; `--query` for custom).
    Postgres(PgArgs),
    /// memcached benchmark (memtier-canonical defaults).
    Memcache(McArgs),
    /// MySQL/MariaDB benchmark (sysbench OLTP-shaped).
    Mysql(MyArgs),
    /// S3 / MinIO object-store benchmark (warp-shaped workloads).
    S3(S3Args),
    /// TinyIce / Icecast2 streaming benchmark (listeners, sources, mixed).
    Tinyice(TinyIceArgs),
}

#[derive(Args, Debug, Clone)]
struct HttpArgs {
    /// Target URL (http:// or https://).
    url: String,
    /// HTTP method.
    #[arg(short = 'm', long = "method", default_value = "GET")]
    method: String,
    /// Request header (repeatable): -H "Name: Value".
    #[arg(short = 'H', long = "header")]
    headers: Vec<String>,
    /// Request body as a literal string.
    #[arg(short = 'd', long = "body")]
    body: Option<String>,
    /// Skip TLS certificate verification (DANGEROUS — testing only).
    #[arg(short = 'k', long = "insecure")]
    insecure: bool,
    /// Force HTTP/2 (h2c for plaintext, ALPN-first for TLS).
    #[arg(long = "http2")]
    http2: bool,
    /// Disable HTTP keep-alive (one request per connection).
    #[arg(long = "no-keepalive")]
    no_keepalive: bool,
}

#[derive(Args, Debug, Clone)]
struct PingArgs {
    /// Hostname or IP to ping.
    host: String,
    /// Payload size in bytes.
    #[arg(short = 's', long = "size", default_value_t = 56)]
    size: usize,
    /// IP TTL.
    #[arg(long = "ttl")]
    ttl: Option<u32>,
    /// Prefer IPv6 when resolving.
    #[arg(short = '6', long = "ipv6")]
    ipv6: bool,
}

#[derive(Args, Debug, Clone)]
struct TcpArgs {
    /// host:port.
    target: String,
}

#[derive(Args, Debug, Clone)]
struct DnsArgs {
    /// Hostname to resolve.
    name: String,
}

#[derive(Args, Debug, Clone)]
struct RedisArgs {
    /// host[:port], or full redis:// / rediss:// URL.
    target: String,
    /// Redis DB number to sandbox into. Default 15 (least likely to collide).
    #[arg(long = "db", default_value_t = 15)]
    db: u8,
    /// AUTH username (Redis 6+ ACL).
    #[arg(long = "user")]
    user: Option<String>,
    /// AUTH password. Prefer the env var to keep it out of `ps`.
    #[arg(long = "password", env = "DATABENCH_REDIS_PASSWORD", hide_env_values = true)]
    password: Option<String>,
    /// Force TLS even if the URL doesn't say rediss://.
    #[arg(long = "tls")]
    tls: bool,
    /// Workload preset: read | memtier (default) | mixed | write.
    /// `memtier` matches memtier_benchmark's canonical 1:10 SET:GET mix.
    #[arg(long = "workload", default_value = "memtier")]
    workload: String,
    /// Number of keys to seed in the sandbox. memtier_benchmark canon
    /// is ~1M; default 100k seeds in a few seconds and is enough to
    /// avoid single-cache-line artifacts.
    #[arg(long = "seed-keys", default_value_t = 100_000)]
    seed_keys: usize,
    /// Value size (bytes) for seeded keys. 32 = memtier_benchmark default.
    #[arg(long = "seed-value-size", default_value_t = 32)]
    seed_value_size: usize,
    /// TTL on every seeded key, in seconds. Belt-and-suspenders cleanup.
    #[arg(long = "seed-ttl", default_value_t = 3600)]
    seed_ttl: u64,
    /// Run a custom command on every probe (e.g. `--cmd "GET foo"`).
    /// Bypasses the workload mix.
    #[arg(long = "cmd")]
    cmd: Option<String>,
    /// Skip sandbox setup and cleanup. Use with --cmd or --workload read
    /// against an existing dataset.
    #[arg(long = "no-sandbox")]
    no_sandbox: bool,
}

#[derive(Args, Debug, Clone)]
struct TinyIceArgs {
    /// Server URL — http://host[:port] or https://host[:port].
    endpoint: String,
    /// Mode: listen | source | mixed (default `listen`).
    #[arg(long = "mode", default_value = "listen")]
    mode: String,
    /// Mount points (comma-separated). Workers round-robin across them.
    /// Default `/live`.
    #[arg(long = "mounts", default_value = "/live", value_delimiter = ',')]
    mounts: Vec<String>,
    /// Username for SOURCE auth. Icecast usually accepts an empty user.
    #[arg(long = "source-user", default_value = "source")]
    source_user: String,
    /// Source password (or default source password). Required for source
    /// and mixed modes.
    #[arg(long = "source-password", env = "DATABENCH_TINYICE_PASSWORD", hide_env_values = true)]
    source_password: Option<String>,
    /// Bitrate (kbps) — used to pace `SOURCE` writes so they don't blast
    /// data faster than a real broadcaster.
    #[arg(long = "source-bitrate", default_value_t = 128)]
    source_bitrate: u32,
    /// Number of source workers in `source` and `mixed` modes. In `listen`
    /// mode this is ignored — `-c` controls the listener count.
    #[arg(long = "sources", default_value_t = 1)]
    sources: usize,
    /// Skip TLS certificate verification. DANGEROUS — testing only.
    #[arg(short = 'k', long = "insecure")]
    insecure: bool,
}

#[derive(Args, Debug, Clone)]
struct S3Args {
    /// Endpoint URL — e.g. http://127.0.0.1:9000 (MinIO local) or
    /// https://s3.amazonaws.com.
    endpoint: String,
    /// Region. MinIO accepts anything; defaults to us-east-1.
    #[arg(long = "region", default_value = "us-east-1")]
    region: String,
    /// Access key. MinIO default is `minioadmin`.
    #[arg(long = "access-key", default_value = "minioadmin", env = "DATABENCH_S3_ACCESS_KEY", hide_env_values = true)]
    access_key: String,
    /// Secret key. MinIO default is `minioadmin`.
    #[arg(long = "secret-key", default_value = "minioadmin", env = "DATABENCH_S3_SECRET_KEY", hide_env_values = true)]
    secret_key: String,
    /// Workload preset: read | mixed (default, warp shape) | write | stat.
    /// `mixed` = 45% GET / 30% STAT / 15% PUT / 10% DELETE.
    #[arg(long = "workload", default_value = "mixed")]
    workload: String,
    /// Object size in bytes. 64 KiB by default — small enough for fast
    /// seeding, big enough to be a real payload. Bump to 10485760 (10 MB)
    /// to mirror warp's defaults.
    #[arg(long = "object-size", default_value_t = 65_536)]
    object_size: usize,
    /// Number of objects to seed.
    #[arg(long = "seed-objects", default_value_t = 1_000)]
    seed_objects: usize,
    /// Skip sandbox; use --bucket on existing data.
    #[arg(long = "no-sandbox")]
    no_sandbox: bool,
    /// Bucket override. With --no-sandbox, this is the bucket worked on.
    /// Without, it's used as the literal bucket name (databench refuses to
    /// auto-clean any bucket whose name doesn't start with `databench-`).
    #[arg(long = "bucket")]
    bucket: Option<String>,
}

#[derive(Args, Debug, Clone)]
struct MyArgs {
    /// host[:port]; default port 3306.
    target: String,
    /// Database to connect to / fall back into. Sandbox is created
    /// outside this when permissions allow.
    #[arg(long = "db", default_value = "mysql")]
    db: String,
    #[arg(long = "user", default_value = "root")]
    user: String,
    #[arg(long = "password", env = "DATABENCH_MYSQL_PASSWORD", hide_env_values = true)]
    password: Option<String>,
    /// Workload preset: point-select | read-only | oltp (default) | write-only.
    /// `oltp` = sysbench's `oltp_read_write`, the canonical MySQL number.
    #[arg(long = "workload", default_value = "oltp")]
    workload: String,
    /// Rows in `sbtest1`. Default 10k = ~2 MB, fits in any buffer pool.
    /// Set --table-size 1000000+ to push past the buffer pool.
    #[arg(long = "table-size", default_value_t = 10_000)]
    table_size: i64,
    /// Run a custom query on every probe. No allow-list; user owns it.
    #[arg(long = "query")]
    query: Option<String>,
    /// Skip sandbox setup and cleanup.
    #[arg(long = "no-sandbox")]
    no_sandbox: bool,
}

#[derive(Args, Debug, Clone)]
struct McArgs {
    /// host[:port]; default port 11211.
    target: String,
    /// Workload preset: read | memtier (default) | mixed | write.
    #[arg(long = "workload", default_value = "memtier")]
    workload: String,
    /// Number of keys to seed.
    #[arg(long = "seed-keys", default_value_t = 100_000)]
    seed_keys: usize,
    /// Value size (bytes) for seeded keys. 32 = memtier_benchmark default.
    #[arg(long = "seed-value-size", default_value_t = 32)]
    seed_value_size: usize,
    /// TTL on every seeded key (seconds).
    #[arg(long = "seed-ttl", default_value_t = 600)]
    seed_ttl: u32,
    /// Run a custom `get <key>` or `delete <key>` on every probe.
    #[arg(long = "cmd")]
    cmd: Option<String>,
    /// Skip sandbox setup and cleanup.
    #[arg(long = "no-sandbox")]
    no_sandbox: bool,
}

#[derive(Args, Debug, Clone)]
struct PgArgs {
    /// Connection target: full `postgresql://user:pass@host:port/db` URL,
    /// or just `host[:port]`.
    target: String,
    /// Database to connect to for setup. The sandbox is created INSIDE
    /// this database if we lack CREATE DATABASE permission.
    #[arg(long = "db", default_value = "postgres")]
    db: String,
    #[arg(long = "user", default_value = "postgres")]
    user: String,
    #[arg(long = "password", env = "DATABENCH_PG_PASSWORD", hide_env_values = true)]
    password: Option<String>,
    /// pgbench scale factor. 1 → 100k accounts, 10 tellers, 1 branch.
    #[arg(long = "scale", default_value_t = 1)]
    scale: i32,
    /// Workload preset: select-only | tpcb (default) | insert.
    #[arg(long = "workload", default_value = "tpcb")]
    workload: String,
    /// Run a custom SELECT/SHOW/EXPLAIN query on every probe.
    #[arg(long = "query")]
    query: Option<String>,
    /// Skip sandbox setup and cleanup. Use only with --query against
    /// existing tables.
    #[arg(long = "no-sandbox")]
    no_sandbox: bool,
}

#[derive(Args, Debug, Clone)]
struct TlsArgs {
    /// host:port.
    target: String,
    /// SNI override.
    #[arg(long = "sni")]
    sni: Option<String>,
    /// Skip TLS certificate verification (DANGEROUS — testing only).
    #[arg(short = 'k', long = "insecure")]
    insecure: bool,
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

struct DisplayInfo {
    kind: BenchKind,
    target: String,
    detail: String,
    threads: usize,
    connections: usize,
}

/// Describes how to undo whatever a bench module created at setup time
/// (seed keys, temp schemas, etc.). Always runs once after workers join,
/// even on Ctrl-C.
enum Cleanup {
    None,
    Redis {
        url: String,
        prefix: String,
    },
    Postgres(postgres_bench::SandboxInfo),
    Memcache(memcache_bench::McConfig),
    Mysql(mysql_bench::SandboxInfo),
    S3 {
        client: aws_sdk_s3::Client,
        bucket: String,
    },
}

impl Cleanup {
    async fn run(self) -> Result<()> {
        match self {
            Cleanup::None => Ok(()),
            Cleanup::Redis { url, prefix } => {
                let (_, mut conn) = redis_bench::connect(&url).await?;
                let cfg = redis_bench::RedisConfig {
                    url: url.clone(),
                    prefix: prefix.clone(),
                    workload: redis_bench::RedisWorkload::Read,
                    seed_keys: 0,
                    seed_value_size: 0,
                    seed_ttl: Duration::from_secs(0),
                    timeout: Duration::from_secs(30),
                    custom: None,
                };
                let n = redis_bench::cleanup_sandbox(&cfg, &mut conn).await?;
                eprintln!("databench: cleaned up {n} redis keys with prefix '{prefix}'");
                Ok(())
            }
            Cleanup::Postgres(info) => {
                postgres_bench::cleanup_sandbox(&info).await?;
                match info.kind {
                    postgres_bench::SandboxKind::Database => {
                        eprintln!("databench: dropped database {}", info.db);
                    }
                    postgres_bench::SandboxKind::Schema => {
                        if let Some(s) = &info.schema {
                            eprintln!("databench: dropped schema {} from {}", s, info.db);
                        }
                    }
                }
                Ok(())
            }
            Cleanup::Memcache(cfg) => {
                let n = memcache_bench::cleanup_sandbox(&cfg).await?;
                eprintln!(
                    "databench: deleted {n} memcache keys with prefix '{}'",
                    cfg.prefix
                );
                Ok(())
            }
            Cleanup::Mysql(info) => {
                mysql_bench::cleanup_sandbox(&info).await?;
                if matches!(info.kind, mysql_bench::SandboxKind::Database) {
                    eprintln!("databench: dropped database `{}`", info.db);
                }
                Ok(())
            }
            Cleanup::S3 { client, bucket } => {
                let n = s3_bench::cleanup(&client, &bucket).await?;
                eprintln!("databench: deleted {n} S3 objects + bucket `{bucket}`");
                Ok(())
            }
        }
    }
}

async fn async_main(cli: Cli, threads: usize) -> Result<()> {
    if cli.requests.is_some() && cli.duration.is_some() {
        eprintln!("note: --requests overrides --duration");
    }
    let (duration, request_budget) = match (cli.requests, cli.duration) {
        (Some(n), _) => (None, n as i64),
        (None, Some(d)) => (Some(d), -1i64),
        (None, None) => (Some(Duration::from_secs(10)), -1i64),
    };

    let live = Arc::new(LiveStats::default());
    let stop = Arc::new(AtomicBool::new(false));
    let stop_notify = Arc::new(tokio::sync::Notify::new());
    let remaining = Arc::new(AtomicI64::new(request_budget));
    let proto_observed = Arc::new(Mutex::new(None::<&'static str>));

    let signal_stop = {
        let stop = stop.clone();
        let notify = stop_notify.clone();
        move || {
            stop.store(true, Ordering::Relaxed);
            notify.notify_waiters();
        }
    };

    // Ctrl+C handler.
    {
        let signal = signal_stop.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            signal();
        });
    }

    let warmup = cli.warmup.unwrap_or(Duration::ZERO);

    // Duration deadline. With --warmup, we sleep warmup+duration so the
    // measurement window is exactly `duration` long.
    if let Some(dur) = duration {
        let signal = signal_stop.clone();
        let total = warmup + dur;
        tokio::spawn(async move {
            tokio::time::sleep(total).await;
            signal();
        });
    }

    let qps = cli.qps.and_then(ratelimit::RateLimiter::from_qps);
    // Recording starts immediately if no warmup was requested.
    let recording = Arc::new(AtomicBool::new(warmup.is_zero()));
    let h = WorkerHandles {
        stop: stop.clone(),
        stop_notify: stop_notify.clone(),
        remaining: remaining.clone(),
        live: live.clone(),
        qps,
        recording: recording.clone(),
    };

    // After the warmup window, zero the live counters and flip
    // `recording` so workers start counting probes against the budget.
    if !warmup.is_zero() {
        let live = live.clone();
        let recording = recording.clone();
        tokio::spawn(async move {
            tokio::time::sleep(warmup).await;
            live.reset();
            recording.store(true, Ordering::Relaxed);
        });
    }

    let started_at = Instant::now();

    let (kind, display, handles, cleanup): (
        BenchKind,
        DisplayInfo,
        Vec<JoinHandle<WorkerReport>>,
        Cleanup,
    ) = match cli.cmd.clone() {
        Cmd::Http(args) => {
            let (k, d, h2) = spawn_http(&cli, args, h.clone(), proto_observed.clone()).await?;
            (k, d, h2, Cleanup::None)
        }
        Cmd::Ping(args) => {
            let (k, d, h2) = spawn_ping(&cli, args, h.clone()).await?;
            (k, d, h2, Cleanup::None)
        }
        Cmd::Tcp(args) => {
            let (k, d, h2) = spawn_tcp(&cli, args, h.clone())?;
            (k, d, h2, Cleanup::None)
        }
        Cmd::Dns(args) => {
            let (k, d, h2) = spawn_dns(&cli, args, h.clone())?;
            (k, d, h2, Cleanup::None)
        }
        Cmd::Tls(args) => {
            let (k, d, h2) = spawn_tls(&cli, args, h.clone(), proto_observed.clone())?;
            (k, d, h2, Cleanup::None)
        }
        Cmd::Redis(args) => spawn_redis(&cli, args, h.clone()).await?,
        Cmd::Postgres(args) => spawn_postgres(&cli, args, h.clone()).await?,
        Cmd::Memcache(args) => spawn_memcache(&cli, args, h.clone()).await?,
        Cmd::Mysql(args) => spawn_mysql(&cli, args, h.clone()).await?,
        Cmd::S3(args) => spawn_s3(&cli, args, h.clone()).await?,
        Cmd::Tinyice(args) => spawn_tinyice(&cli, args, h.clone())?,
    };

    let display = DisplayInfo {
        threads,
        ..display
    };

    // Backstop: if stop has fired and one second of grace passes,
    // hard-abort any worker still running so a hung probe can't drag
    // the run on indefinitely. Workers that exit cleanly within the
    // grace period contribute their full reports; aborted ones return
    // JoinError and are silently dropped.
    {
        let stop_notify = stop_notify.clone();
        let abort_handles: Vec<_> = handles.iter().map(|h| h.abort_handle()).collect();
        tokio::spawn(async move {
            stop_notify.notified().await;
            tokio::time::sleep(Duration::from_millis(1000)).await;
            for h in abort_handles {
                h.abort();
            }
        });
    }

    // Live UI.
    let use_tui = !cli.no_tui && stdout().is_terminal();
    if use_tui {
        run_tui(
            &display,
            started_at,
            duration,
            cli.requests,
            live.clone(),
            stop.clone(),
            stop_notify.clone(),
            proto_observed.clone(),
            &handles,
        )
        .await?;
    } else {
        run_plain(
            kind,
            started_at,
            duration,
            cli.requests,
            live.clone(),
            stop.clone(),
            &handles,
        )
        .await;
    }

    signal_stop();
    let mut reports = Vec::with_capacity(handles.len());
    for h in handles {
        if let Ok(r) = h.await {
            reports.push(r);
        }
    }

    if let Err(e) = cleanup.run().await {
        eprintln!("databench: cleanup failed: {e}");
    }

    // Subtract the warmup window so RPS / total reflect measured time only.
    let total_duration = started_at.elapsed().saturating_sub(warmup);
    let default_proto: &'static str = match kind {
        BenchKind::Http => "HTTP/1.1",
        BenchKind::Ping => "ICMP",
        BenchKind::Tcp => "TCP",
        BenchKind::Dns => "DNS (system resolver)",
        BenchKind::Tls => "TLS",
        BenchKind::Redis => "RESP",
        BenchKind::Postgres => "PostgreSQL wire",
        BenchKind::Mysql => "MySQL wire",
        BenchKind::Memcache => "memcached text",
        BenchKind::S3 => "S3 / HTTPS",
        BenchKind::TinyIce => "Icecast2",
    };
    let protocol = proto_observed.lock().unwrap_or(default_proto);
    let final_report =
        FinalReport::from_workers(kind, reports, total_duration, cli.connections, protocol);

    print!("{}", render_final(&final_report));
    stdout().flush().ok();
    Ok(())
}

// ---------- bench dispatchers ---------------------------------------------

async fn spawn_http(
    cli: &Cli,
    args: HttpArgs,
    h: WorkerHandles,
    proto_observed: Arc<Mutex<Option<&'static str>>>,
) -> Result<(BenchKind, DisplayInfo, Vec<JoinHandle<WorkerReport>>)> {
    let target = parse_http_target(&args.url)?;
    let method: http::Method = args
        .method
        .parse()
        .with_context(|| format!("invalid HTTP method: {}", args.method))?;
    let headers = parse_headers(&args.headers)?;
    let body = args.body.unwrap_or_default().into_bytes().into();

    let cfg = Arc::new(WorkerConfig {
        target: Arc::new(target),
        method: method.clone(),
        headers: Arc::new(headers),
        body,
        timeout: cli.timeout,
        keepalive: !args.no_keepalive,
        force_h2: args.http2,
        insecure: args.insecure,
    });

    let mut handles: Vec<JoinHandle<WorkerReport>> = Vec::with_capacity(cli.connections);
    for _ in 0..cli.connections {
        let cfg = cfg.clone();
        let h = h.clone();
        let proto = proto_observed.clone();
        handles.push(tokio::spawn(async move {
            run_http_worker(cfg, h, proto).await
        }));
    }

    let detail = format!(
        "method={}  keep-alive={}  http2={}",
        method.as_str(),
        if args.no_keepalive { "off" } else { "on" },
        if args.http2 { "forced" } else { "negotiated" }
    );
    Ok((
        BenchKind::Http,
        DisplayInfo {
            kind: BenchKind::Http,
            target: args.url,
            detail,
            threads: 0,
            connections: cli.connections,
        },
        handles,
    ))
}

async fn spawn_ping(
    cli: &Cli,
    args: PingArgs,
    h: WorkerHandles,
) -> Result<(BenchKind, DisplayInfo, Vec<JoinHandle<WorkerReport>>)> {
    let addr = ping_bench::resolve_target(&args.host, args.ipv6).await?;
    let client = ping_bench::build_client(addr, args.ttl)?;
    let cfg = Arc::new(ping_bench::PingConfig {
        host: args.host.clone(),
        addr,
        payload_size: args.size,
        ttl: args.ttl,
        timeout: cli.timeout,
    });
    let handles =
        ping_bench::spawn_workers(Arc::new(client), cfg, cli.connections, h);
    let detail = format!(
        "addr={}  size={}B{}",
        addr,
        args.size,
        args.ttl.map(|t| format!("  ttl={t}")).unwrap_or_default(),
    );
    Ok((
        BenchKind::Ping,
        DisplayInfo {
            kind: BenchKind::Ping,
            target: args.host,
            detail,
            threads: 0,
            connections: cli.connections,
        },
        handles,
    ))
}

fn spawn_tcp(
    cli: &Cli,
    args: TcpArgs,
    h: WorkerHandles,
) -> Result<(BenchKind, DisplayInfo, Vec<JoinHandle<WorkerReport>>)> {
    let (host, port) = parse_host_port(&args.target)?;
    let cfg = Arc::new(tcp_bench::TcpConfig {
        host: host.clone(),
        port,
        timeout: cli.timeout,
    });
    let handles = tcp_bench::spawn_workers(cfg, cli.connections, h);
    let detail = format!("host={host}  port={port}");
    Ok((
        BenchKind::Tcp,
        DisplayInfo {
            kind: BenchKind::Tcp,
            target: args.target,
            detail,
            threads: 0,
            connections: cli.connections,
        },
        handles,
    ))
}

fn spawn_dns(
    cli: &Cli,
    args: DnsArgs,
    h: WorkerHandles,
) -> Result<(BenchKind, DisplayInfo, Vec<JoinHandle<WorkerReport>>)> {
    let cfg = Arc::new(dns_bench::DnsConfig {
        name: args.name.clone(),
        timeout: cli.timeout,
    });
    let handles = dns_bench::spawn_workers(cfg, cli.connections, h);
    Ok((
        BenchKind::Dns,
        DisplayInfo {
            kind: BenchKind::Dns,
            target: args.name,
            detail: "system resolver (lookup_host)".to_string(),
            threads: 0,
            connections: cli.connections,
        },
        handles,
    ))
}

async fn spawn_redis(
    cli: &Cli,
    args: RedisArgs,
    h: WorkerHandles,
) -> Result<(
    BenchKind,
    DisplayInfo,
    Vec<JoinHandle<WorkerReport>>,
    Cleanup,
)> {
    let url = redis_bench::build_url(
        &args.target,
        Some(args.db),
        args.user.as_deref(),
        args.password.as_deref(),
        args.tls,
    );

    let workload = redis_bench::RedisWorkload::parse(&args.workload)?;
    let custom = args.cmd.as_ref().map(|s| {
        // crude split — quote-aware would be better, but this is consistent
        // with what redis-cli sees from the same shell.
        s.split_whitespace().map(|s| s.to_string()).collect::<Vec<_>>()
    });

    let run_id = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let prefix = format!("databench:{run_id}:");

    let cfg = Arc::new(redis_bench::RedisConfig {
        url: url.clone(),
        prefix: prefix.clone(),
        workload,
        seed_keys: args.seed_keys,
        seed_value_size: args.seed_value_size,
        seed_ttl: Duration::from_secs(args.seed_ttl),
        timeout: cli.timeout,
        custom: custom.clone(),
    });

    // Smoke probe + sandbox setup before forking workers.
    let (_client, mut conn) = redis_bench::connect(&url)
        .await
        .with_context(|| format!("connecting to redis at {}", redact_url(&url)))?;
    redis_bench::pre_run_checks(&mut conn).await.ok();
    let cleanup = if !args.no_sandbox && custom.is_none() {
        eprintln!(
            "databench: seeding {} keys (prefix={}, value_size={}B) ...",
            args.seed_keys, prefix, args.seed_value_size
        );
        redis_bench::setup_sandbox(&cfg, &mut conn)
            .await
            .context("seeding redis sandbox")?;
        Cleanup::Redis {
            url: url.clone(),
            prefix: prefix.clone(),
        }
    } else {
        Cleanup::None
    };

    let handles = redis_bench::spawn_workers(cfg.clone(), cli.connections, h);

    let detail = match (&custom, args.no_sandbox) {
        (Some(c), _) => format!("custom cmd={:?}  db={}", c, args.db),
        (None, true) => format!("workload={}  no-sandbox  db={}", args.workload, args.db),
        (None, false) => format!(
            "workload={}  seed_keys={}  db={}  prefix={}",
            args.workload, args.seed_keys, args.db, prefix
        ),
    };

    Ok((
        BenchKind::Redis,
        DisplayInfo {
            kind: BenchKind::Redis,
            target: redact_url(&url),
            detail,
            threads: 0,
            connections: cli.connections,
        },
        handles,
        cleanup,
    ))
}

fn spawn_tinyice(
    cli: &Cli,
    args: TinyIceArgs,
    h: WorkerHandles,
) -> Result<(
    BenchKind,
    DisplayInfo,
    Vec<JoinHandle<WorkerReport>>,
    Cleanup,
)> {
    let parsed = url::Url::parse(&args.endpoint)
        .with_context(|| format!("invalid URL: {}", args.endpoint))?;
    let use_tls = match parsed.scheme() {
        "http" => false,
        "https" => true,
        other => bail!("unsupported scheme '{other}' (expected http or https)"),
    };
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("URL must have a host"))?
        .to_string();
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| anyhow!("unable to determine port"))?;

    let mode = tinyice_bench::TinyIceMode::parse(&args.mode)?;
    let mounts: Vec<String> = args
        .mounts
        .iter()
        .map(|m| {
            if m.starts_with('/') {
                m.clone()
            } else {
                format!("/{}", m)
            }
        })
        .collect();
    if mounts.is_empty() {
        bail!("--mounts must list at least one mount");
    }

    let listener_count = cli.connections;
    let source_count = match mode {
        tinyice_bench::TinyIceMode::Source => cli.connections,
        tinyice_bench::TinyIceMode::Mixed => args.sources.max(1),
        tinyice_bench::TinyIceMode::Listen => 0,
    };

    let needs_password =
        matches!(mode, tinyice_bench::TinyIceMode::Source | tinyice_bench::TinyIceMode::Mixed);
    let source_password = match (needs_password, args.source_password.as_deref()) {
        (true, Some(p)) => p.to_string(),
        (true, None) => bail!(
            "source/mixed modes need --source-password (or DATABENCH_TINYICE_PASSWORD)"
        ),
        _ => String::new(),
    };

    if matches!(mode, tinyice_bench::TinyIceMode::Source) && source_count > mounts.len() {
        eprintln!(
            "note: --sources ({}) > --mounts ({}); workers will round-robin and multiple sources will collide on the same mount.",
            source_count,
            mounts.len()
        );
    }

    let cfg = Arc::new(tinyice_bench::TinyIceConfig {
        host: host.clone(),
        port,
        use_tls,
        insecure: args.insecure,
        mounts: mounts.clone(),
        mode,
        source_user: args.source_user.clone(),
        source_password,
        source_bitrate_kbps: args.source_bitrate,
        timeout: cli.timeout,
        source_t0: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
    });

    let handles = tinyice_bench::spawn(cfg.clone(), listener_count, source_count, h);

    let target_label = format!(
        "{}://{}:{}{}",
        if use_tls { "https" } else { "http" },
        host,
        port,
        if mounts.len() == 1 {
            mounts[0].clone()
        } else {
            format!(" ({} mounts)", mounts.len())
        }
    );
    let detail = match mode {
        tinyice_bench::TinyIceMode::Listen => format!(
            "mode=listen  listeners={}  mounts={}",
            listener_count,
            mounts.join(",")
        ),
        tinyice_bench::TinyIceMode::Source => format!(
            "mode=source  sources={}  bitrate={}kbps  mounts={}",
            source_count,
            args.source_bitrate,
            mounts.join(",")
        ),
        tinyice_bench::TinyIceMode::Mixed => format!(
            "mode=mixed  sources={}  listeners={}  bitrate={}kbps  mounts={}",
            source_count,
            listener_count,
            args.source_bitrate,
            mounts.join(",")
        ),
    };

    Ok((
        BenchKind::TinyIce,
        DisplayInfo {
            kind: BenchKind::TinyIce,
            target: target_label,
            detail,
            threads: 0,
            connections: listener_count + source_count,
        },
        handles,
        Cleanup::None,
    ))
}

async fn spawn_s3(
    cli: &Cli,
    args: S3Args,
    h: WorkerHandles,
) -> Result<(
    BenchKind,
    DisplayInfo,
    Vec<JoinHandle<WorkerReport>>,
    Cleanup,
)> {
    let workload = s3_bench::S3Workload::parse(&args.workload)?;
    let run_id = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let bucket = args
        .bucket
        .clone()
        .unwrap_or_else(|| format!("databench-{run_id}"));

    let cfg = s3_bench::S3Config {
        endpoint: args.endpoint.clone(),
        region: args.region.clone(),
        bucket: bucket.clone(),
        access_key: args.access_key.clone(),
        secret_key: args.secret_key.clone(),
        object_size: args.object_size,
        seed_objects: args.seed_objects,
        workload,
        timeout: cli.timeout,
    };
    let client = s3_bench::build_client(&cfg).context("building S3 client")?;
    let payload = bytes::Bytes::from(vec![0u8; args.object_size]);

    let cleanup = if args.no_sandbox {
        Cleanup::None
    } else {
        eprintln!(
            "databench: creating S3 bucket `{bucket}` and seeding {} objects of {} bytes ...",
            args.seed_objects, args.object_size
        );
        s3_bench::create_bucket(&client, &bucket).await?;
        if matches!(workload, s3_bench::S3Workload::Read | s3_bench::S3Workload::Mixed | s3_bench::S3Workload::Stat) {
            s3_bench::populate(&client, &bucket, args.seed_objects, payload.clone()).await?;
        }
        Cleanup::S3 {
            client: client.clone(),
            bucket: bucket.clone(),
        }
    };

    let arc_cfg = Arc::new(cfg);
    let handles =
        s3_bench::spawn_workers(client, arc_cfg.clone(), payload, cli.connections, h);

    let detail = match (args.no_sandbox, &args.bucket) {
        (true, _) => format!(
            "workload={}  no-sandbox  bucket={}",
            args.workload,
            bucket
        ),
        (false, _) => format!(
            "workload={}  obj_size={}B  seed={}  bucket={}",
            args.workload, args.object_size, args.seed_objects, bucket
        ),
    };

    Ok((
        BenchKind::S3,
        DisplayInfo {
            kind: BenchKind::S3,
            target: args.endpoint,
            detail,
            threads: 0,
            connections: cli.connections,
        },
        handles,
        cleanup,
    ))
}

async fn spawn_mysql(
    cli: &Cli,
    args: MyArgs,
    h: WorkerHandles,
) -> Result<(
    BenchKind,
    DisplayInfo,
    Vec<JoinHandle<WorkerReport>>,
    Cleanup,
)> {
    let (host, port) = if args.target.contains(':') {
        parse_host_port(&args.target)?
    } else {
        (args.target.clone(), 3306u16)
    };
    let workload = mysql_bench::MyWorkload::parse(&args.workload)?;
    let admin_opts = mysql_bench::build_opts(
        &host,
        port,
        &args.user,
        args.password.as_deref(),
        Some(&args.db),
    );

    let run_id = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();

    let (info, cleanup) = if args.no_sandbox {
        (
            mysql_bench::SandboxInfo {
                kind: mysql_bench::SandboxKind::None,
                admin_opts: admin_opts.clone(),
                db: args.db.clone(),
            },
            Cleanup::None,
        )
    } else {
        eprintln!(
            "databench: setting up mysql sandbox (table_size={}) ...",
            args.table_size
        );
        let info =
            mysql_bench::setup_sandbox(admin_opts.clone(), &args.db, &run_id).await?;
        if args.query.is_none() {
            let worker_opts = mysql_bench::build_opts(
                &host,
                port,
                &args.user,
                args.password.as_deref(),
                Some(&info.db),
            );
            mysql_bench::populate(worker_opts, args.table_size).await?;
        }
        let cleanup = match info.kind {
            mysql_bench::SandboxKind::Database => Cleanup::Mysql(info.clone()),
            mysql_bench::SandboxKind::None => Cleanup::None,
        };
        (info, cleanup)
    };

    let worker_opts = mysql_bench::build_opts(
        &host,
        port,
        &args.user,
        args.password.as_deref(),
        Some(&info.db),
    );
    let cfg = Arc::new(mysql_bench::MyConfig {
        opts: worker_opts,
        sandbox_db: info.db.clone(),
        sandbox_kind: info.kind,
        workload,
        table_size: args.table_size,
        timeout: cli.timeout,
        custom_query: args.query.clone(),
    });

    let handles = mysql_bench::spawn_workers(cfg.clone(), cli.connections, h);
    let detail = match (&args.query, args.no_sandbox) {
        (Some(q), _) => format!("custom query=\"{}\"", truncate(q, 60)),
        (None, true) => format!("workload={}  no-sandbox", args.workload),
        (None, false) => format!(
            "workload={}  table_size={}",
            args.workload, args.table_size
        ),
    };

    Ok((
        BenchKind::Mysql,
        DisplayInfo {
            kind: BenchKind::Mysql,
            target: format!("mysql://{host}:{port}/{}", info.db),
            detail,
            threads: 0,
            connections: cli.connections,
        },
        handles,
        cleanup,
    ))
}

async fn spawn_memcache(
    cli: &Cli,
    args: McArgs,
    h: WorkerHandles,
) -> Result<(
    BenchKind,
    DisplayInfo,
    Vec<JoinHandle<WorkerReport>>,
    Cleanup,
)> {
    let (host, port) = if args.target.contains(':') {
        parse_host_port(&args.target)?
    } else {
        (args.target.clone(), 11211)
    };
    let workload = memcache_bench::McWorkload::parse(&args.workload)?;

    let run_id = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let prefix = format!("databench:{run_id}:");

    let cfg = memcache_bench::McConfig {
        host: host.clone(),
        port,
        prefix: prefix.clone(),
        workload,
        seed_keys: args.seed_keys,
        seed_value_size: args.seed_value_size,
        seed_ttl: args.seed_ttl,
        timeout: cli.timeout,
        custom: args.cmd.clone(),
    };

    // Smoke test + sandbox.
    let mut client = memcache_bench::McClient::connect(&host, port)
        .await
        .with_context(|| format!("connect memcached {host}:{port}"))?;
    let version = client.version().await.unwrap_or_else(|_| "unknown".into());
    let cleanup = if !args.no_sandbox && args.cmd.is_none() {
        eprintln!(
            "databench: seeding {} memcache keys (prefix={}, value_size={}B) ...",
            args.seed_keys, prefix, args.seed_value_size
        );
        memcache_bench::setup_sandbox(&cfg, &mut client)
            .await
            .context("seeding memcache sandbox")?;
        Cleanup::Memcache(cfg.clone())
    } else {
        Cleanup::None
    };
    drop(client);

    let arc_cfg = Arc::new(cfg);
    let handles = memcache_bench::spawn_workers(arc_cfg.clone(), cli.connections, h);

    let detail = match (&args.cmd, args.no_sandbox) {
        (Some(c), _) => format!("custom cmd=\"{}\"  v={}", truncate(c, 50), version),
        (None, true) => format!("workload={}  no-sandbox  v={}", args.workload, version),
        (None, false) => format!(
            "workload={}  seed_keys={}  v={}",
            args.workload, args.seed_keys, version
        ),
    };

    Ok((
        BenchKind::Memcache,
        DisplayInfo {
            kind: BenchKind::Memcache,
            target: format!("memcached://{host}:{port}"),
            detail,
            threads: 0,
            connections: cli.connections,
        },
        handles,
        cleanup,
    ))
}

async fn spawn_postgres(
    cli: &Cli,
    args: PgArgs,
    h: WorkerHandles,
) -> Result<(
    BenchKind,
    DisplayInfo,
    Vec<JoinHandle<WorkerReport>>,
    Cleanup,
)> {
    let workload = postgres_bench::PgWorkload::parse(&args.workload)?;

    // pgbench's well-known guidance: when `scale < clients`, every worker
    // contends on the same handful of branches/tellers rows and you end
    // up measuring lock contention rather than the database. Refuse the
    // run with a clear message instead of silently publishing a
    // misleading number.
    if args.query.is_none()
        && !args.no_sandbox
        && matches!(workload, postgres_bench::PgWorkload::Tpcb)
        && (args.scale as usize) < cli.connections
    {
        bail!(
            "pgbench TPC-B contends badly when scale ({}) < connections ({}). \
             Bump --scale to at least {} or pick `--workload select-only`.",
            args.scale,
            cli.connections,
            cli.connections
        );
    }

    // Resolve a usable URL. If user gave host[:port], glue together.
    let server_url = if args.target.contains("://") {
        args.target.clone()
    } else {
        let (host, port) = if let Some((h, p)) = args.target.rsplit_once(':') {
            let port: u16 = p.parse().with_context(|| format!("port: {p}"))?;
            (h.to_string(), port)
        } else {
            (args.target.clone(), 5432u16)
        };
        postgres_bench::build_url(
            &host,
            port,
            &args.user,
            args.password.as_deref(),
            &args.db,
        )
    };

    let run_id = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();

    let (sandbox_info, cleanup) = if args.no_sandbox {
        // Run against the user's database and schema directly. Custom
        // queries usually go this way.
        let info = postgres_bench::SandboxInfo {
            kind: postgres_bench::SandboxKind::Schema,
            db: args.db.clone(),
            schema: None,
            server_url: server_url.clone(),
        };
        (info, Cleanup::None)
    } else {
        eprintln!("databench: setting up postgres sandbox (scale={}) ...", args.scale);
        let info = postgres_bench::setup_sandbox(&server_url, &args.db, &run_id).await?;
        // Seed only when not running a custom query.
        if args.query.is_none() {
            let url = postgres_bench::worker_url(&info);
            let client = postgres_bench::connect(&url).await?;
            postgres_bench::populate(&client, args.scale).await?;
        }
        let cleanup = Cleanup::Postgres(info.clone());
        (info, cleanup)
    };

    let cfg = Arc::new(postgres_bench::PgConfig {
        server_url: postgres_bench::worker_url(&sandbox_info),
        sandbox_db: sandbox_info.db.clone(),
        sandbox_schema: sandbox_info.schema.clone(),
        workload,
        scale: args.scale,
        timeout: cli.timeout,
        custom_query: args.query.clone(),
    });

    let handles = postgres_bench::spawn_workers(cfg.clone(), cli.connections, h);

    let target_label = format!("{}{}",
        match (&sandbox_info.kind, &sandbox_info.schema) {
            (postgres_bench::SandboxKind::Database, _) => format!("postgres://.../{}", sandbox_info.db),
            (postgres_bench::SandboxKind::Schema, Some(s)) => {
                format!("postgres://.../{}#{}", sandbox_info.db, s)
            }
            _ => format!("postgres://.../{}", sandbox_info.db),
        },
        ""
    );
    let detail = match (&args.query, args.no_sandbox) {
        (Some(q), _) => format!("custom query=\"{}\"", truncate(q, 60)),
        (None, true) => format!("workload={}  no-sandbox", args.workload),
        (None, false) => format!("workload={}  scale={}", args.workload, args.scale),
    };

    Ok((
        BenchKind::Postgres,
        DisplayInfo {
            kind: BenchKind::Postgres,
            target: target_label,
            detail,
            threads: 0,
            connections: cli.connections,
        },
        handles,
        cleanup,
    ))
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max).collect();
        out.push('…');
        out
    }
}

/// Strip user:password from a URL-form target so it never lands on stdout.
fn redact_url(url: &str) -> String {
    if let Ok(mut u) = url::Url::parse(url) {
        let _ = u.set_password(None);
        let _ = u.set_username("");
        return u.to_string();
    }
    url.to_string()
}

fn spawn_tls(
    cli: &Cli,
    args: TlsArgs,
    h: WorkerHandles,
    proto_observed: Arc<Mutex<Option<&'static str>>>,
) -> Result<(BenchKind, DisplayInfo, Vec<JoinHandle<WorkerReport>>)> {
    let (host, port) = parse_host_port(&args.target)?;
    let cfg = Arc::new(tls_bench::TlsBenchConfig {
        host: host.clone(),
        port,
        sni: args.sni.clone(),
        insecure: args.insecure,
        timeout: cli.timeout,
    });
    let handles = tls_bench::spawn_workers(cfg, cli.connections, h, proto_observed);
    let detail = format!(
        "host={host}  port={port}  sni={}",
        args.sni.unwrap_or_else(|| host.clone())
    );
    Ok((
        BenchKind::Tls,
        DisplayInfo {
            kind: BenchKind::Tls,
            target: args.target,
            detail,
            threads: 0,
            connections: cli.connections,
        },
        handles,
    ))
}

// ---------- parsing helpers -----------------------------------------------

fn parse_http_target(input: &str) -> Result<Target> {
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
    let full = format!(
        "{}://{}{}",
        parsed.scheme(),
        authority,
        path_uri.path_and_query().map(|p| p.as_str()).unwrap_or("")
    );
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

fn parse_host_port(raw: &str) -> Result<(String, u16)> {
    let (host, port) = raw
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("expected host:port, got: {raw}"))?;
    let port: u16 = port
        .parse()
        .with_context(|| format!("invalid port: {port}"))?;
    if host.is_empty() {
        bail!("missing host in {raw}");
    }
    Ok((host.to_string(), port))
}

// ---------- TUI ------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_tui(
    display: &DisplayInfo,
    started_at: Instant,
    duration: Option<Duration>,
    request_target: Option<u64>,
    live: Arc<LiveStats>,
    stop: Arc<AtomicBool>,
    stop_notify: Arc<tokio::sync::Notify>,
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
        display,
        started_at,
        duration,
        request_target,
        &live,
        &stop,
        &stop_notify,
        &proto,
        handles,
        &mut last_snapshot,
        &mut last_tick,
        &mut rps_history,
        &mut last_progress_at,
    )
    .await;

    let mut out = stdout();
    let _ = execute!(out, LeaveAlternateScreen);
    let _ = disable_raw_mode();
    result
}

#[allow(clippy::too_many_arguments)]
async fn tui_loop(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    display: &DisplayInfo,
    started_at: Instant,
    duration: Option<Duration>,
    request_target: Option<u64>,
    live: &Arc<LiveStats>,
    stop: &Arc<AtomicBool>,
    stop_notify: &Arc<tokio::sync::Notify>,
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
                display,
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

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    let ctrl_c = key.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('C'));
                    if ctrl_c
                        || matches!(key.code, KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc)
                    {
                        stop.store(true, Ordering::Relaxed);
                        stop_notify.notify_waiters();
                    }
                }
            }
        }

        if handles.iter().all(|h| h.is_finished()) {
            break;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn draw_dashboard(
    frame: &mut ratatui::Frame<'_>,
    display: &DisplayInfo,
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
            Constraint::Length(4),
            Constraint::Length(3),
            Constraint::Length(8),
            Constraint::Min(10),
            Constraint::Length(1),
        ])
        .split(area);

    draw_header(frame, chunks[0], display, proto);
    draw_progress(
        frame,
        chunks[1],
        display.kind,
        elapsed,
        duration,
        request_target,
        snap.requests,
        stalled,
        stalled_for,
    );
    draw_body(frame, chunks[2], display.kind, snap, current_rps, current_bps);
    draw_chart(
        frame,
        chunks[3],
        display.kind,
        rps_history,
        current_rps,
        stalled,
    );
    draw_footer(frame, chunks[4]);
}

fn draw_header(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    display: &DisplayInfo,
    proto: &'static str,
) {
    let title = format!(
        " databench v{}  ·  {} ",
        env!("CARGO_PKG_VERSION"),
        display.kind.label()
    );
    let block = Block::default()
        .borders(Borders::ALL)
        .title(Span::styled(
            title,
            Style::default().add_modifier(Modifier::BOLD).fg(Color::Cyan),
        ));
    let line1 = Line::from(vec![
        Span::styled("Target  ", Style::default().fg(Color::DarkGray)),
        Span::styled(display.target.clone(), Style::default().fg(Color::White)),
    ]);
    let line2 = Line::from(vec![
        Span::styled(display.detail.clone(), Style::default().fg(Color::Gray)),
        Span::raw("   "),
        Span::styled("Proto ", Style::default().fg(Color::DarkGray)),
        Span::styled(proto, Style::default().fg(Color::Yellow)),
        Span::raw("   "),
        Span::styled("Workers ", Style::default().fg(Color::DarkGray)),
        Span::raw(display.connections.to_string()),
        Span::raw("   "),
        Span::styled("Threads ", Style::default().fg(Color::DarkGray)),
        Span::raw(display.threads.to_string()),
    ]);
    let p = Paragraph::new(vec![line1, line2]).block(block);
    frame.render_widget(p, area);
}

#[allow(clippy::too_many_arguments)]
fn draw_progress(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    kind: BenchKind,
    elapsed: Duration,
    duration: Option<Duration>,
    request_target: Option<u64>,
    requests: u64,
    stalled: bool,
    stalled_for: Duration,
) {
    let unit = kind.unit_plural();
    let (mut label, ratio) = match (duration, request_target) {
        (_, Some(n)) => {
            let r = (requests as f64 / n.max(1) as f64).clamp(0.0, 1.0);
            (format!("{}/{}  {}", fmt_int(requests), fmt_int(n), unit), r)
        }
        (Some(d), _) => {
            let r = (elapsed.as_secs_f64() / d.as_secs_f64().max(1e-9)).clamp(0.0, 1.0);
            (format!("{} / {}", fmt_clock(elapsed), fmt_clock(d)), r)
        }
        _ => (fmt_clock(elapsed), 0.0),
    };

    let (gauge_color, title) = if stalled {
        label.push_str(&format!(
            "   ⚠ STALLED for {}s — no completed {}",
            stalled_for.as_secs(),
            unit
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
    kind: BenchKind,
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

    let rps_color = if rps > 10_000.0 {
        Color::Green
    } else if rps > 1_000.0 {
        Color::Yellow
    } else if rps > 0.0 {
        Color::White
    } else {
        Color::Red
    };
    let unit = kind.unit_plural();
    let lines = vec![
        big_kv(
            &format!("{}/sec   ", unit),
            format!("{:>14.1}", rps),
            rps_color,
        ),
        big_kv("Bytes/sec  ", format!("{:>14}", fmt_bytes_rate(bps)), Color::Cyan),
        big_kv("Total      ", format!("{:>14}", fmt_int(snap.requests)), Color::White),
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

    // Phase averages — labels filtered by bench kind so unused rows don't
    // show up as "—".
    let dns = LiveSnapshot::avg(snap.dns_us_sum, snap.dns_count);
    let tcp = LiveSnapshot::avg(snap.tcp_us_sum, snap.tcp_count);
    let tls = LiveSnapshot::avg(snap.tls_us_sum, snap.tls_count);
    let hs = LiveSnapshot::avg(snap.hs_us_sum, snap.hs_count);
    let ttfb = LiveSnapshot::avg(snap.ttfb_us_sum, snap.ttfb_count);
    let body = LiveSnapshot::avg(snap.body_us_sum, snap.body_count);

    let phase_lines: Vec<Line<'static>> = match kind {
        BenchKind::Http => vec![
            phase_kv("DNS lookup    ", dns, Color::Magenta),
            phase_kv("TCP connect   ", tcp, Color::Blue),
            phase_kv("TLS handshake ", tls, Color::Cyan),
            phase_kv("HTTP handshake", hs, Color::Gray),
            phase_kv("TTFB          ", ttfb, Color::Yellow),
            phase_kv("Body download ", body, Color::Green),
        ],
        BenchKind::Tls => vec![
            phase_kv("DNS lookup    ", dns, Color::Magenta),
            phase_kv("TCP connect   ", tcp, Color::Blue),
            phase_kv("TLS handshake ", tls, Color::Cyan),
        ],
        BenchKind::Tcp => vec![
            phase_kv("DNS lookup    ", dns, Color::Magenta),
            phase_kv("TCP connect   ", tcp, Color::Blue),
        ],
        BenchKind::Dns => vec![phase_kv("DNS lookup    ", dns, Color::Magenta)],
        BenchKind::Ping => vec![phase_kv("Round-trip    ", ttfb, Color::Yellow)],
        BenchKind::Redis | BenchKind::Memcache | BenchKind::S3 => {
            vec![phase_kv("Op RTT        ", ttfb, Color::Yellow)]
        }
        BenchKind::Postgres | BenchKind::Mysql => {
            vec![phase_kv("Txn time      ", ttfb, Color::Yellow)]
        }
        BenchKind::TinyIce => {
            vec![phase_kv("Connect+TTFB  ", ttfb, Color::Yellow)]
        }
    };
    let phases = Paragraph::new(phase_lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" phase averages "),
    );
    frame.render_widget(phases, cols[1]);

    // Right column: HTTP shows status codes, others show ok/err.
    let total = snap.requests.max(1);
    let pct = |n: u64| n as f64 / total as f64 * 100.0;
    let status_lines: Vec<Line<'static>> = match kind {
        BenchKind::Http => vec![
            status_line("1xx", snap.status_1xx, pct(snap.status_1xx), Color::Gray),
            status_line("2xx", snap.status_2xx, pct(snap.status_2xx), Color::Green),
            status_line("3xx", snap.status_3xx, pct(snap.status_3xx), Color::Cyan),
            status_line("4xx", snap.status_4xx, pct(snap.status_4xx), Color::Yellow),
            status_line("5xx", snap.status_5xx, pct(snap.status_5xx), Color::Red),
            status_line("err", snap.errors, pct(snap.errors), Color::Magenta),
        ],
        _ => {
            let ok = snap.requests.saturating_sub(snap.errors);
            vec![
                status_line(" ok", ok, pct(ok), Color::Green),
                status_line("err", snap.errors, pct(snap.errors), Color::Red),
            ]
        }
    };
    let title = match kind {
        BenchKind::Http => " responses ",
        BenchKind::Ping => " pings ",
        BenchKind::Redis | BenchKind::Memcache | BenchKind::S3 => " ops ",
        BenchKind::Postgres | BenchKind::Mysql => " txns ",
        BenchKind::TinyIce => " streams ",
        _ => " probes ",
    };
    let status = Paragraph::new(status_lines)
        .block(Block::default().borders(Borders::ALL).title(title));
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
    kind: BenchKind,
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
    let y_max = nice_ceiling((peak as f64).max(current_rps).max(1.0));
    let data: Vec<(f64, f64)> = rps_history
        .iter()
        .enumerate()
        .map(|(i, v)| (i as f64, *v as f64))
        .collect();

    let line_color = if stalled { Color::Red } else { Color::Green };
    let datasets = vec![Dataset::default()
        .name(kind.unit_plural())
        .marker(Marker::Braille)
        .graph_type(GraphType::Line)
        .style(Style::default().fg(line_color))
        .data(&data)];

    let title = format!(
        " {}/sec   now {:.0}    avg {:.0}    peak {} ",
        kind.unit_plural(),
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
    kind: BenchKind,
    started_at: Instant,
    duration: Option<Duration>,
    request_target: Option<u64>,
    live: Arc<LiveStats>,
    stop: Arc<AtomicBool>,
    handles: &[JoinHandle<WorkerReport>],
) {
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
            "\r[{}] {}={:<10} rps={:<10.1} bytes={:<10} errs={:<6}",
            progress,
            kind.unit_plural(),
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
