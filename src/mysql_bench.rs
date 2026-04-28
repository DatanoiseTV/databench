//! MySQL / MariaDB benchmark — `sysbench` OLTP-shaped.
//!
//! Sandbox: try `CREATE DATABASE databench_<runid>`, fall back to creating
//! prefixed tables in the user's `--db` if we lack the privilege. Schema
//! follows sysbench's standard `sbtest1`. Workloads are modelled on
//! `sysbench oltp_*` so the per-statement breakdown is comparable to what
//! published MySQL numbers reference.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Pool};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::task::JoinHandle;

use crate::stats::WorkerReport;
use crate::worker::{try_reserve_budget, WorkerHandles};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MyWorkload {
    /// 10 point selects, no surrounding transaction. Lightest read.
    PointSelect,
    /// sysbench `oltp_read_only`: 10 point selects + 4 range queries
    /// inside one transaction.
    ReadOnly,
    /// sysbench `oltp_read_write` (default): point selects + ranges +
    /// updates + delete + insert in one transaction. The number that
    /// matters for OLTP MySQL.
    Oltp,
    /// sysbench `oltp_write_only`: 2 updates + 1 delete + 1 insert.
    WriteOnly,
}

impl MyWorkload {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "point-select" | "point" | "select" => Ok(Self::PointSelect),
            "read-only" | "read" | "ro" => Ok(Self::ReadOnly),
            "oltp" | "read-write" | "rw" | "mixed" => Ok(Self::Oltp),
            "write-only" | "write" | "wo" => Ok(Self::WriteOnly),
            other => Err(anyhow!(
                "unknown workload '{other}'; expected point-select | read-only | oltp | write-only"
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MyConfig {
    /// Connection options used by every worker (with the sandbox DB set).
    pub opts: Opts,
    /// Sandbox database name; freshly created or existing.
    pub sandbox_db: String,
    pub sandbox_kind: SandboxKind,
    pub workload: MyWorkload,
    pub table_size: i64,
    pub timeout: Duration,
    pub custom_query: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxKind {
    Database,
    None,
}

#[derive(Clone, Debug)]
pub struct SandboxInfo {
    pub kind: SandboxKind,
    pub admin_opts: Opts,
    pub db: String,
}

pub fn build_opts(
    host: &str,
    port: u16,
    user: &str,
    password: Option<&str>,
    db: Option<&str>,
) -> Opts {
    OptsBuilder::default()
        .ip_or_hostname(host.to_string())
        .tcp_port(port)
        .user(Some(user.to_string()))
        .pass(password.map(|s| s.to_string()))
        .db_name(db.map(|s| s.to_string()))
        .stmt_cache_size(64)
        .into()
}

pub async fn setup_sandbox(
    admin_opts: Opts,
    fallback_db: &str,
    run_id: &str,
) -> Result<SandboxInfo> {
    let mut admin = Conn::new(admin_opts.clone()).await.context("connect")?;
    let db = format!("databench_{run_id}");
    let create = format!("CREATE DATABASE `{db}`");
    match admin.query_drop(&create).await {
        Ok(()) => Ok(SandboxInfo {
            kind: SandboxKind::Database,
            admin_opts,
            db,
        }),
        Err(_e) => {
            // Either we lack CREATE perms, or DB already exists. Re-raise
            // any other class of error.
            // Try to fall back to operating in the user-supplied DB.
            eprintln!(
                "databench: CREATE DATABASE failed; running in existing db `{fallback_db}` (no sandbox)"
            );
            Ok(SandboxInfo {
                kind: SandboxKind::None,
                admin_opts,
                db: fallback_db.to_string(),
            })
        }
    }
}

pub async fn cleanup_sandbox(info: &SandboxInfo) -> Result<()> {
    if info.kind != SandboxKind::Database {
        return Ok(());
    }
    if !info.db.starts_with("databench_") {
        return Err(anyhow!(
            "refusing to drop unexpected database '{}'",
            info.db
        ));
    }
    let mut admin = Conn::new(info.admin_opts.clone()).await?;
    admin
        .query_drop(format!("DROP DATABASE IF EXISTS `{}`", info.db))
        .await?;
    Ok(())
}

pub async fn populate(opts: Opts, table_size: i64) -> Result<()> {
    let mut conn = Conn::new(opts).await.context("populate connect")?;
    eprintln!("databench: mysql sandbox CREATE TABLE ...");

    conn.query_drop(
        r#"
        CREATE TABLE sbtest1 (
            id INT NOT NULL AUTO_INCREMENT,
            k INT NOT NULL DEFAULT 0,
            c CHAR(120) NOT NULL DEFAULT '',
            pad CHAR(60) NOT NULL DEFAULT '',
            PRIMARY KEY (id),
            KEY k_1 (k)
        )"#,
    )
    .await
    .context("CREATE TABLE")?;

    // Bulk insert in batches. Batch size kept conservative so wire frames
    // stay under MariaDB's max_allowed_packet (default 16 MB) for any
    // reasonable --table-size.
    let batch = 1_000usize;
    let mut rng = SmallRng::seed_from_u64(0xBABE_FACE);
    let mut row = 1i64;
    while row <= table_size {
        let end = (row + batch as i64 - 1).min(table_size);
        let mut sql = String::from("INSERT INTO sbtest1(id, k, c, pad) VALUES ");
        for i in row..=end {
            if i > row {
                sql.push(',');
            }
            let k: i32 = rng.gen_range(0..i32::MAX / 2);
            sql.push_str(&format!(
                "({}, {}, '{}', '{}')",
                i,
                k,
                pad_str(120),
                pad_str(60),
            ));
        }
        conn.query_drop(&sql).await.context("seed insert")?;
        row = end + 1;
    }
    eprintln!(
        "databench: mysql seeded {} rows; running ANALYZE",
        table_size
    );
    conn.query_drop("ANALYZE TABLE sbtest1").await.ok();
    let _ = conn.disconnect().await;
    Ok(())
}

fn pad_str(n: usize) -> String {
    // Fixed deterministic-ish filler; sysbench uses random-looking
    // strings, but we just want a payload of the right size.
    "x".repeat(n)
}

pub fn spawn_workers(
    cfg: Arc<MyConfig>,
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

async fn run_worker(cfg: Arc<MyConfig>, h: WorkerHandles, worker_idx: u64) -> WorkerReport {
    let mut report = WorkerReport::new();
    let mut conn = match Conn::new(cfg.opts.clone()).await {
        Ok(c) => c,
        Err(e) => {
            report.record_error(&h.live, &format!("connect: {}", e));
            return report;
        }
    };
    let mut rng = SmallRng::seed_from_u64(0xFACE_FEED ^ worker_idx);

    while !h.stop.load(Ordering::Relaxed) {
        let measuring = h.measuring();
        if measuring && try_reserve_budget(&h.remaining).is_none() {
            break;
        }
        h.rate_gate().await;

        let result = if let Some(q) = &cfg.custom_query {
            run_custom(&mut conn, q, cfg.timeout).await
        } else {
            match cfg.workload {
                MyWorkload::PointSelect => {
                    run_point_select(&mut conn, &mut rng, cfg.table_size, cfg.timeout).await
                }
                MyWorkload::ReadOnly => {
                    run_read_only(&mut conn, &mut rng, cfg.table_size, cfg.timeout).await
                }
                MyWorkload::Oltp => run_oltp(&mut conn, &mut rng, cfg.table_size, cfg.timeout).await,
                MyWorkload::WriteOnly => {
                    run_write_only(&mut conn, &mut rng, cfg.table_size, cfg.timeout).await
                }
            }
        };

        if !measuring {
            if result.is_err() {
                if let Ok(c) = Conn::new(cfg.opts.clone()).await {
                    conn = c;
                }
            }
            continue;
        }
        match result {
            Ok(ops) => {
                let total: u64 = ops.iter().map(|(_, us)| *us).sum();
                report.record_txn(&ops);
                h.live.requests.fetch_add(1, Ordering::Relaxed);
                h.live.record_request_phases(total, 0);
            }
            Err(e) => {
                report.record_error(&h.live, &format!("mysql: {}", e));
                if let Ok(c) = Conn::new(cfg.opts.clone()).await {
                    conn = c;
                }
            }
        }
    }
    report
}

const POINT_SELECTS_PER_TXN: usize = 10;
const RANGE_SIZE: i64 = 100;

async fn timed<F, T>(label: &str, fut: F, timeout: Duration) -> Result<(String, u64, T)>
where
    F: std::future::Future<Output = mysql_async::Result<T>>,
{
    let start = Instant::now();
    let v = match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return Err(anyhow!("{}", e)),
        Err(_) => return Err(anyhow!("timeout")),
    };
    let us = start.elapsed().as_micros() as u64;
    Ok((label.to_string(), us, v))
}

async fn run_point_select(
    conn: &mut Conn,
    rng: &mut SmallRng,
    table_size: i64,
    timeout: Duration,
) -> Result<Vec<(String, u64)>> {
    let mut steps = Vec::with_capacity(POINT_SELECTS_PER_TXN);
    for _ in 0..POINT_SELECTS_PER_TXN {
        let id = rng.gen_range(1..=table_size);
        let (lbl, us, _r): (String, u64, Vec<String>) = timed(
            "point-select",
            conn.exec("SELECT c FROM sbtest1 WHERE id = ?", (id,)),
            timeout,
        )
        .await?;
        steps.push((lbl, us));
    }
    Ok(steps)
}

async fn run_read_only(
    conn: &mut Conn,
    rng: &mut SmallRng,
    table_size: i64,
    timeout: Duration,
) -> Result<Vec<(String, u64)>> {
    let mut steps = Vec::with_capacity(15);
    let (lbl, us, _r): (String, u64, ()) =
        timed("begin", conn.query_drop("BEGIN"), timeout).await?;
    steps.push((lbl, us));
    for _ in 0..POINT_SELECTS_PER_TXN {
        let id = rng.gen_range(1..=table_size);
        let (lbl, us, _r): (String, u64, Vec<String>) = timed(
            "point-select",
            conn.exec("SELECT c FROM sbtest1 WHERE id = ?", (id,)),
            timeout,
        )
        .await?;
        steps.push((lbl, us));
    }
    let id = rng.gen_range(1..=(table_size - RANGE_SIZE).max(1));
    let (lbl, us, _r): (String, u64, Vec<String>) = timed(
        "simple-range",
        conn.exec(
            "SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?",
            (id, id + RANGE_SIZE),
        ),
        timeout,
    )
    .await?;
    steps.push((lbl, us));
    let id = rng.gen_range(1..=(table_size - RANGE_SIZE).max(1));
    let (lbl, us, _r): (String, u64, Vec<i64>) = timed(
        "sum-range",
        conn.exec(
            "SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?",
            (id, id + RANGE_SIZE),
        ),
        timeout,
    )
    .await?;
    steps.push((lbl, us));
    let id = rng.gen_range(1..=(table_size - RANGE_SIZE).max(1));
    let (lbl, us, _r): (String, u64, Vec<String>) = timed(
        "ordered-range",
        conn.exec(
            "SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c",
            (id, id + RANGE_SIZE),
        ),
        timeout,
    )
    .await?;
    steps.push((lbl, us));
    let id = rng.gen_range(1..=(table_size - RANGE_SIZE).max(1));
    let (lbl, us, _r): (String, u64, Vec<String>) = timed(
        "distinct-range",
        conn.exec(
            "SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN ? AND ? ORDER BY c",
            (id, id + RANGE_SIZE),
        ),
        timeout,
    )
    .await?;
    steps.push((lbl, us));
    let (lbl, us, _r): (String, u64, ()) =
        timed("commit", conn.query_drop("COMMIT"), timeout).await?;
    steps.push((lbl, us));
    Ok(steps)
}

async fn run_write_only(
    conn: &mut Conn,
    rng: &mut SmallRng,
    table_size: i64,
    timeout: Duration,
) -> Result<Vec<(String, u64)>> {
    let mut steps = Vec::with_capacity(6);
    let (lbl, us, _r): (String, u64, ()) =
        timed("begin", conn.query_drop("BEGIN"), timeout).await?;
    steps.push((lbl, us));
    let id = rng.gen_range(1..=table_size);
    let (lbl, us, _r): (String, u64, ()) = timed(
        "update-index",
        conn.exec_drop("UPDATE sbtest1 SET k = k + 1 WHERE id = ?", (id,)),
        timeout,
    )
    .await?;
    steps.push((lbl, us));
    let id = rng.gen_range(1..=table_size);
    let new_c = pad_str(120);
    let (lbl, us, _r): (String, u64, ()) = timed(
        "update-non-index",
        conn.exec_drop(
            "UPDATE sbtest1 SET c = ? WHERE id = ?",
            (new_c.as_str(), id),
        ),
        timeout,
    )
    .await?;
    steps.push((lbl, us));
    let id = rng.gen_range(1..=table_size);
    let (lbl, us, _r): (String, u64, ()) = timed(
        "delete",
        conn.exec_drop("DELETE FROM sbtest1 WHERE id = ?", (id,)),
        timeout,
    )
    .await?;
    steps.push((lbl, us));
    let k: i32 = rng.gen_range(0..i32::MAX / 2);
    let pad120 = pad_str(120);
    let pad60 = pad_str(60);
    // Let the server pick the id via AUTO_INCREMENT — much safer than
    // racing for synthetic ids across workers.
    let (lbl, us, _r): (String, u64, ()) = timed(
        "insert",
        conn.exec_drop(
            "INSERT INTO sbtest1(k, c, pad) VALUES (?, ?, ?)",
            (k, pad120.as_str(), pad60.as_str()),
        ),
        timeout,
    )
    .await?;
    steps.push((lbl, us));
    let (lbl, us, _r): (String, u64, ()) =
        timed("commit", conn.query_drop("COMMIT"), timeout).await?;
    steps.push((lbl, us));
    Ok(steps)
}

async fn run_oltp(
    conn: &mut Conn,
    rng: &mut SmallRng,
    table_size: i64,
    timeout: Duration,
) -> Result<Vec<(String, u64)>> {
    let mut all = run_read_only(conn, rng, table_size, timeout).await?;
    // Pop the closing COMMIT so we can append writes inside the same txn.
    if matches!(all.last(), Some((s, _)) if s == "commit") {
        all.pop();
    }
    let mut writes = run_writes_in_open_txn(conn, rng, table_size, timeout).await?;
    all.append(&mut writes);
    let (lbl, us, _r): (String, u64, ()) =
        timed("commit", conn.query_drop("COMMIT"), timeout).await?;
    all.push((lbl, us));
    Ok(all)
}

async fn run_writes_in_open_txn(
    conn: &mut Conn,
    rng: &mut SmallRng,
    table_size: i64,
    timeout: Duration,
) -> Result<Vec<(String, u64)>> {
    let mut steps = Vec::with_capacity(4);
    let id = rng.gen_range(1..=table_size);
    let (lbl, us, _r): (String, u64, ()) = timed(
        "update-index",
        conn.exec_drop("UPDATE sbtest1 SET k = k + 1 WHERE id = ?", (id,)),
        timeout,
    )
    .await?;
    steps.push((lbl, us));
    let id = rng.gen_range(1..=table_size);
    let new_c = pad_str(120);
    let (lbl, us, _r): (String, u64, ()) = timed(
        "update-non-index",
        conn.exec_drop(
            "UPDATE sbtest1 SET c = ? WHERE id = ?",
            (new_c.as_str(), id),
        ),
        timeout,
    )
    .await?;
    steps.push((lbl, us));
    let id = rng.gen_range(1..=table_size);
    let (lbl, us, _r): (String, u64, ()) = timed(
        "delete",
        conn.exec_drop("DELETE FROM sbtest1 WHERE id = ?", (id,)),
        timeout,
    )
    .await?;
    steps.push((lbl, us));
    let k: i32 = rng.gen_range(0..i32::MAX / 2);
    let pad120 = pad_str(120);
    let pad60 = pad_str(60);
    // Let the server pick the id via AUTO_INCREMENT — much safer than
    // racing for synthetic ids across workers.
    let (lbl, us, _r): (String, u64, ()) = timed(
        "insert",
        conn.exec_drop(
            "INSERT INTO sbtest1(k, c, pad) VALUES (?, ?, ?)",
            (k, pad120.as_str(), pad60.as_str()),
        ),
        timeout,
    )
    .await?;
    steps.push((lbl, us));
    Ok(steps)
}

async fn run_custom(
    conn: &mut Conn,
    query: &str,
    timeout: Duration,
) -> Result<Vec<(String, u64)>> {
    let start = Instant::now();
    let _: () = match tokio::time::timeout(timeout, conn.query_drop(query)).await {
        Ok(Ok(())) => (),
        Ok(Err(e)) => return Err(anyhow!("{}", e)),
        Err(_) => return Err(anyhow!("timeout")),
    };
    Ok(vec![("custom".to_string(), start.elapsed().as_micros() as u64)])
}
