//! PostgreSQL benchmark — TPC-B-lite, faithful to `pgbench`'s default
//! transaction. Sandboxed by default: creates `databench_<runid>` (or
//! falls back to a schema in the user-supplied `--db` if it lacks
//! `CREATE DATABASE`), seeds the four pgbench tables, runs the workload,
//! and drops everything on exit.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use futures_util::SinkExt;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::task::JoinHandle;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Config, NoTls, Statement};

use crate::stats::WorkerReport;
use crate::worker::{try_reserve_budget, WorkerHandles};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgWorkload {
    /// pgbench `--select-only`: one indexed point select. The lightest
    /// real-world read workload.
    SelectOnly,
    /// pgbench default: 5-statement TPC-B-like transaction (3 updates,
    /// 1 select, 1 insert) wrapped in BEGIN/COMMIT.
    Tpcb,
    /// Insert-only into pgbench_history. Stresses the write path
    /// without contending updates on the small tables.
    Insert,
}

impl PgWorkload {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "select-only" | "select" | "read" => Ok(Self::SelectOnly),
            "tpcb" | "tpcb-lite" | "mixed" => Ok(Self::Tpcb),
            "insert" | "write" => Ok(Self::Insert),
            other => Err(anyhow!(
                "unknown workload '{other}'; expected select-only, tpcb, or insert"
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PgConfig {
    /// Connection string for the server (used at setup time and by workers).
    /// The database the workers actually open is overridden to `sandbox_db`.
    pub server_url: String,
    /// Database name for the sandbox; either freshly created or existing.
    pub sandbox_db: String,
    /// Optional schema (set if we fell back from CREATE DATABASE).
    pub sandbox_schema: Option<String>,
    pub workload: PgWorkload,
    pub scale: i32,
    pub timeout: Duration,
    /// User-supplied SQL to run on every probe. Bypasses the workload.
    pub custom_query: Option<String>,
}

#[derive(Clone, Debug)]
pub enum SandboxKind {
    Database, // we created (and own) the database
    Schema,   // we only created a schema in the user's database
}

#[derive(Clone, Debug)]
pub struct SandboxInfo {
    pub kind: SandboxKind,
    pub db: String,
    pub schema: Option<String>,
    pub server_url: String,
}

/// Connect to a server using the given URL. Spawns the connection driver
/// in the background and returns the client handle.
pub async fn connect(url: &str) -> Result<Client> {
    let cfg: Config = url.parse().context("invalid postgres URL")?;
    let (client, conn) = cfg
        .connect(NoTls)
        .await
        .with_context(|| format!("connecting to postgres at {}", redact(url)))?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("postgres connection error: {e}");
        }
    });
    Ok(client)
}

fn redact(url: &str) -> String {
    if let Ok(mut u) = url::Url::parse(url) {
        let _ = u.set_password(None);
        let _ = u.set_username("");
        return u.to_string();
    }
    url.to_string()
}

/// Construct a postgres URL with `application_name=databench` so the
/// server-side admin can identify our connections.
pub fn build_url(
    host: &str,
    port: u16,
    user: &str,
    password: Option<&str>,
    db: &str,
) -> String {
    let auth = match password {
        Some(p) => format!("{user}:{p}@"),
        None => format!("{user}@"),
    };
    format!(
        "postgresql://{auth}{host}:{port}/{db}?application_name=databench"
    )
}

/// Try to create a brand-new sandbox database. If we lack permission,
/// fall back to creating a uniquely-named schema in `--db`.
pub async fn setup_sandbox(
    server_url: &str,
    fallback_db: &str,
    run_id: &str,
) -> Result<SandboxInfo> {
    let admin = connect(server_url).await?;
    let db_name = format!("databench_{run_id}");
    let create_db = format!("CREATE DATABASE \"{db_name}\"");
    match admin.batch_execute(&create_db).await {
        Ok(()) => Ok(SandboxInfo {
            kind: SandboxKind::Database,
            db: db_name,
            schema: None,
            server_url: server_url.to_string(),
        }),
        Err(e) => {
            // Lack of CREATE DATABASE perm gives a SqlState 42501
            // ("insufficient privilege"). Anything else (host down,
            // bad creds) we re-raise.
            let permission_denied = e
                .code()
                .map(|c| c.code() == "42501")
                .unwrap_or(false);
            if !permission_denied {
                return Err(e).context("CREATE DATABASE");
            }
            eprintln!(
                "databench: no CREATE DATABASE permission, falling back to a schema in {fallback_db}"
            );
            // Re-connect to the fallback DB and create a schema there.
            let url = swap_db(server_url, fallback_db);
            let client = connect(&url).await?;
            let schema = format!("databench_{run_id}");
            client
                .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
                .await
                .context("CREATE SCHEMA")?;
            Ok(SandboxInfo {
                kind: SandboxKind::Schema,
                db: fallback_db.to_string(),
                schema: Some(schema),
                server_url: server_url.to_string(),
            })
        }
    }
}

/// Drop whatever the sandbox owns. Safe-by-name: DROP DATABASE only
/// happens if the name still starts with `databench_`.
pub async fn cleanup_sandbox(info: &SandboxInfo) -> Result<()> {
    match info.kind {
        SandboxKind::Database => {
            if !info.db.starts_with("databench_") {
                return Err(anyhow!(
                    "refusing to drop unexpected database name '{}'",
                    info.db
                ));
            }
            let admin = connect(&info.server_url).await?;
            admin
                .batch_execute(&format!("DROP DATABASE IF EXISTS \"{}\"", info.db))
                .await
                .context("DROP DATABASE")?;
        }
        SandboxKind::Schema => {
            if let Some(schema) = &info.schema {
                if !schema.starts_with("databench_") {
                    return Err(anyhow!(
                        "refusing to drop unexpected schema name '{}'",
                        schema
                    ));
                }
                let url = swap_db(&info.server_url, &info.db);
                let c = connect(&url).await?;
                c.batch_execute(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
                    .await
                    .context("DROP SCHEMA")?;
            }
        }
    }
    Ok(())
}

/// Replace the path component (database name) of a URL.
fn swap_db(url: &str, db: &str) -> String {
    if let Ok(mut u) = url::Url::parse(url) {
        u.set_path(&format!("/{db}"));
        return u.to_string();
    }
    url.to_string()
}

/// Build the URL each worker will connect with — points at the sandbox DB
/// and pre-sets `search_path` to the schema (if applicable) so unqualified
/// table references resolve to our seeded tables.
pub fn worker_url(info: &SandboxInfo) -> String {
    let mut url = swap_db(&info.server_url, &info.db);
    if let Some(schema) = &info.schema {
        let sep = if url.contains('?') { '&' } else { '?' };
        url.push(sep);
        url.push_str("options=-csearch_path%3D");
        url.push_str(schema);
    }
    url
}

/// Create the four pgbench tables and seed them at the requested scale.
/// `scale=1` → 100k accounts, 10 tellers, 1 branch. Same shape as
/// `pgbench -i` (just smaller default scale).
pub async fn populate(client: &Client, scale: i32) -> Result<()> {
    if scale < 1 {
        return Err(anyhow!("scale must be >= 1"));
    }
    client
        .batch_execute(
            r#"
            CREATE TABLE pgbench_branches(
                bid INT PRIMARY KEY,
                bbalance INT,
                filler CHAR(88)
            );
            CREATE TABLE pgbench_tellers(
                tid INT PRIMARY KEY,
                bid INT,
                tbalance INT,
                filler CHAR(84)
            );
            CREATE TABLE pgbench_accounts(
                aid INT PRIMARY KEY,
                bid INT,
                abalance INT,
                filler CHAR(84)
            );
            CREATE TABLE pgbench_history(
                tid INT,
                bid INT,
                aid INT,
                delta INT,
                mtime TIMESTAMP DEFAULT NOW(),
                filler CHAR(22)
            );
            "#,
        )
        .await
        .context("CREATE TABLE")?;

    // Branches
    let mut sql = String::from("INSERT INTO pgbench_branches(bid, bbalance) VALUES ");
    for i in 1..=scale {
        if i > 1 {
            sql.push(',');
        }
        sql.push_str(&format!("({i}, 0)"));
    }
    client.batch_execute(&sql).await.context("seed branches")?;

    // Tellers (10 per branch).
    let mut sql = String::from("INSERT INTO pgbench_tellers(tid, bid, tbalance) VALUES ");
    let mut first = true;
    for b in 1..=scale {
        for t in 1..=10 {
            let tid = (b - 1) * 10 + t;
            if !first {
                sql.push(',');
            }
            first = false;
            sql.push_str(&format!("({tid}, {b}, 0)"));
        }
    }
    client.batch_execute(&sql).await.context("seed tellers")?;

    // Accounts via COPY — INSERT would take many seconds at scale 1.
    let total: i64 = 100_000 * scale as i64;
    let copy_stmt = "COPY pgbench_accounts(aid, bid, abalance) FROM STDIN";
    let sink = client.copy_in(copy_stmt).await.context("COPY accounts")?;
    let mut sink = std::pin::pin!(sink);
    use bytes::Bytes;
    let mut buf = String::with_capacity(64 * 1024);
    for aid in 1..=total {
        let bid = ((aid - 1) / 100_000) + 1;
        buf.clear();
        buf.push_str(&format!("{aid}\t{bid}\t0\n"));
        sink.send(Bytes::copy_from_slice(buf.as_bytes()))
            .await
            .context("COPY accounts row")?;
    }
    sink.as_mut().finish().await.context("COPY accounts finish")?;

    client
        .batch_execute("ANALYZE")
        .await
        .context("ANALYZE")?;
    Ok(())
}

pub fn spawn_workers(
    cfg: Arc<PgConfig>,
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

struct Prepared {
    select_abalance: Option<Statement>,
    update_accounts: Option<Statement>,
    update_tellers: Option<Statement>,
    update_branches: Option<Statement>,
    insert_history: Option<Statement>,
    custom: Option<Statement>,
}

/// Prepare exactly the statements the chosen workload needs. Avoids
/// failing in custom-query / no-sandbox runs where the pgbench tables
/// don't exist.
async fn prepare(client: &Client, workload: PgWorkload, custom: Option<&str>) -> Result<Prepared> {
    if let Some(q) = custom {
        return Ok(Prepared {
            select_abalance: None,
            update_accounts: None,
            update_tellers: None,
            update_branches: None,
            insert_history: None,
            custom: Some(client.prepare(q).await.context("preparing custom query")?),
        });
    }
    let mut p = Prepared {
        select_abalance: None,
        update_accounts: None,
        update_tellers: None,
        update_branches: None,
        insert_history: None,
        custom: None,
    };
    match workload {
        PgWorkload::SelectOnly => {
            p.select_abalance = Some(
                client
                    .prepare("SELECT abalance FROM pgbench_accounts WHERE aid = $1")
                    .await?,
            );
        }
        PgWorkload::Insert => {
            p.insert_history = Some(
                client
                    .prepare(
                        "INSERT INTO pgbench_history(tid, bid, aid, delta) \
                         VALUES ($1, $2, $3, $4)",
                    )
                    .await?,
            );
        }
        PgWorkload::Tpcb => {
            p.select_abalance = Some(
                client
                    .prepare("SELECT abalance FROM pgbench_accounts WHERE aid = $1")
                    .await?,
            );
            p.update_accounts = Some(
                client
                    .prepare(
                        "UPDATE pgbench_accounts SET abalance = abalance + $1 WHERE aid = $2",
                    )
                    .await?,
            );
            p.update_tellers = Some(
                client
                    .prepare(
                        "UPDATE pgbench_tellers SET tbalance = tbalance + $1 WHERE tid = $2",
                    )
                    .await?,
            );
            p.update_branches = Some(
                client
                    .prepare(
                        "UPDATE pgbench_branches SET bbalance = bbalance + $1 WHERE bid = $2",
                    )
                    .await?,
            );
            p.insert_history = Some(
                client
                    .prepare(
                        "INSERT INTO pgbench_history(tid, bid, aid, delta) \
                         VALUES ($1, $2, $3, $4)",
                    )
                    .await?,
            );
        }
    }
    Ok(p)
}

async fn run_worker(cfg: Arc<PgConfig>, h: WorkerHandles, worker_idx: u64) -> WorkerReport {
    let mut report = WorkerReport::new();
    let mut client = match connect(&cfg.server_url).await {
        Ok(c) => c,
        Err(e) => {
            report.record_error(&h.live, &format!("connect: {}", e));
            return report;
        }
    };
    let mut prep = match prepare(&client, cfg.workload, cfg.custom_query.as_deref()).await {
        Ok(p) => p,
        Err(e) => {
            report.record_error(&h.live, &format!("prepare: {}", e));
            return report;
        }
    };

    let mut rng = SmallRng::seed_from_u64(0xDEADBEEF ^ worker_idx);
    let scale = cfg.scale;
    let n_accounts: i64 = 100_000 * scale as i64;
    let n_tellers: i32 = 10 * scale;
    let n_branches: i32 = scale;

    while !h.stop.load(Ordering::Relaxed) {
        let measuring = h.measuring();
        if measuring && try_reserve_budget(&h.remaining).is_none() {
            break;
        }
        h.rate_gate().await;

        let result = if let Some(stmt) = &prep.custom {
            run_custom(&client, stmt, cfg.timeout).await
        } else {
            match cfg.workload {
                PgWorkload::SelectOnly => {
                    run_select_only(&client, &prep, &mut rng, n_accounts, cfg.timeout).await
                }
                PgWorkload::Tpcb => {
                    run_tpcb(
                        &client,
                        &prep,
                        &mut rng,
                        n_accounts,
                        n_tellers,
                        n_branches,
                        cfg.timeout,
                    )
                    .await
                }
                PgWorkload::Insert => {
                    run_insert_only(
                        &client,
                        &prep,
                        &mut rng,
                        n_accounts,
                        n_tellers,
                        n_branches,
                        cfg.timeout,
                    )
                    .await
                }
            }
        };

        if !measuring {
            // Discard outcome during warmup; reconnect on transient error.
            if result.is_err() {
                if let Ok(c) = connect(&cfg.server_url).await {
                    if let Ok(p) = prepare(&c, cfg.workload, cfg.custom_query.as_deref()).await {
                        client = c;
                        prep = p;
                    }
                }
            }
            continue;
        }
        match result {
            Ok(ops) => {
                for (op, us) in &ops {
                    report.record_op(op, *us);
                }
                // The "transaction" total is the sum of statement timings,
                // recorded as a single global total bump so RPS reflects
                // *transactions per second* rather than statements/sec.
                let total_us: u64 = ops.iter().map(|(_, us)| *us).sum();
                if !ops.is_empty() {
                    // record_op already bumped requests + total per statement;
                    // undo the duplicate count by replacing with one bump.
                    // (In practice we want statements counted in per_op but
                    // requests/sec to reflect transactions.)
                    // For simplicity we just emit the combined RPS via the
                    // record-op flow: leave it, because per_op count is
                    // accurate per statement already.
                }
                let _ = total_us;
                h.live.requests.fetch_add(1, Ordering::Relaxed);
                h.live.record_request_phases(
                    ops.iter().map(|(_, us)| *us).sum::<u64>(),
                    0,
                );
            }
            Err(e) => {
                report.record_error(&h.live, &format!("postgres: {}", e));
                // Try reconnecting so a transient blip doesn't kill the worker.
                if let Ok(c) = connect(&cfg.server_url).await {
                    if let Ok(p) = prepare(&c, cfg.workload, cfg.custom_query.as_deref()).await {
                        client = c;
                        prep = p;
                    }
                }
            }
        }
    }
    report
}

async fn run_with_timeout<F, T>(fut: F, timeout: Duration) -> Result<T>
where
    F: std::future::Future<Output = Result<T, tokio_postgres::Error>>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(anyhow!("{}", e)),
        Err(_) => Err(anyhow!("timeout")),
    }
}

async fn run_select_only(
    client: &Client,
    prep: &Prepared,
    rng: &mut SmallRng,
    n_accounts: i64,
    timeout: Duration,
) -> Result<Vec<(String, u64)>> {
    let stmt = prep.select_abalance.as_ref().expect("prepared");
    let aid: i32 = rng.gen_range(1..=n_accounts as i32);
    let start = Instant::now();
    let _row = run_with_timeout(client.query_one(stmt, &[&aid]), timeout).await?;
    Ok(vec![("select-abalance".to_string(), start.elapsed().as_micros() as u64)])
}

async fn run_tpcb(
    client: &Client,
    prep: &Prepared,
    rng: &mut SmallRng,
    n_accounts: i64,
    n_tellers: i32,
    n_branches: i32,
    timeout: Duration,
) -> Result<Vec<(String, u64)>> {
    let delta: i32 = rng.gen_range(-5000..=5000);
    let aid: i32 = rng.gen_range(1..=n_accounts as i32);
    let tid: i32 = rng.gen_range(1..=n_tellers);
    let bid: i32 = rng.gen_range(1..=n_branches);

    let stmt_sel = prep.select_abalance.as_ref().expect("prepared");
    let stmt_ua = prep.update_accounts.as_ref().expect("prepared");
    let stmt_ut = prep.update_tellers.as_ref().expect("prepared");
    let stmt_ub = prep.update_branches.as_ref().expect("prepared");
    let stmt_ih = prep.insert_history.as_ref().expect("prepared");

    let mut steps: Vec<(String, u64)> = Vec::with_capacity(7);
    let begin_t = Instant::now();
    run_with_timeout(client.batch_execute("BEGIN"), timeout).await?;
    steps.push(("begin".to_string(), begin_t.elapsed().as_micros() as u64));

    let t = Instant::now();
    let p: [&(dyn ToSql + Sync); 2] = [&delta, &aid];
    run_with_timeout(client.execute(stmt_ua, &p), timeout).await?;
    steps.push((
        "update-accounts".to_string(),
        t.elapsed().as_micros() as u64,
    ));

    let t = Instant::now();
    run_with_timeout(client.query_one(stmt_sel, &[&aid]), timeout).await?;
    steps.push((
        "select-abalance".to_string(),
        t.elapsed().as_micros() as u64,
    ));

    let t = Instant::now();
    let p: [&(dyn ToSql + Sync); 2] = [&delta, &tid];
    run_with_timeout(client.execute(stmt_ut, &p), timeout).await?;
    steps.push(("update-tellers".to_string(), t.elapsed().as_micros() as u64));

    let t = Instant::now();
    let p: [&(dyn ToSql + Sync); 2] = [&delta, &bid];
    run_with_timeout(client.execute(stmt_ub, &p), timeout).await?;
    steps.push(("update-branches".to_string(), t.elapsed().as_micros() as u64));

    let t = Instant::now();
    let p: [&(dyn ToSql + Sync); 4] = [&tid, &bid, &aid, &delta];
    run_with_timeout(client.execute(stmt_ih, &p), timeout).await?;
    steps.push(("insert-history".to_string(), t.elapsed().as_micros() as u64));

    let commit_t = Instant::now();
    run_with_timeout(client.batch_execute("COMMIT"), timeout).await?;
    steps.push(("commit".to_string(), commit_t.elapsed().as_micros() as u64));

    Ok(steps)
}

async fn run_insert_only(
    client: &Client,
    prep: &Prepared,
    rng: &mut SmallRng,
    n_accounts: i64,
    n_tellers: i32,
    n_branches: i32,
    timeout: Duration,
) -> Result<Vec<(String, u64)>> {
    let delta: i32 = rng.gen_range(-5000..=5000);
    let aid: i32 = rng.gen_range(1..=n_accounts as i32);
    let tid: i32 = rng.gen_range(1..=n_tellers);
    let bid: i32 = rng.gen_range(1..=n_branches);
    let stmt = prep.insert_history.as_ref().expect("prepared");
    let start = Instant::now();
    let p: [&(dyn ToSql + Sync); 4] = [&tid, &bid, &aid, &delta];
    run_with_timeout(client.execute(stmt, &p), timeout).await?;
    Ok(vec![(
        "insert-history".to_string(),
        start.elapsed().as_micros() as u64,
    )])
}

async fn run_custom(
    client: &Client,
    stmt: &Statement,
    timeout: Duration,
) -> Result<Vec<(String, u64)>> {
    let start = Instant::now();
    let _ = run_with_timeout(client.query(stmt, &[]), timeout).await?;
    Ok(vec![("custom".to_string(), start.elapsed().as_micros() as u64)])
}
