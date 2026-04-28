//! S3 / MinIO benchmark.
//!
//! Workload defaults follow MinIO's `warp` since that's the canonical
//! S3 benchmark people compare against:
//!
//!   mixed (default) → 45% GET, 30% STAT, 15% PUT, 10% DELETE
//!
//! Sandbox owns its own bucket (`databench-<runid>`), pre-populates a
//! configurable number of objects with a configurable size, runs the
//! workload, then deletes every object and finally the bucket. Cleanup
//! refuses to touch a bucket whose name doesn't start with `databench-`.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use bytes::Bytes;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::task::JoinHandle;

use crate::stats::WorkerReport;
use crate::worker::{try_reserve_budget, WorkerHandles};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum S3Workload {
    /// 100% GET — pure download throughput / object-cache scenario.
    Read,
    /// warp default mix: 45% GET / 30% STAT / 15% PUT / 10% DELETE.
    Mixed,
    /// 100% PUT — pure upload throughput / write-heavy scenario.
    Write,
    /// 100% HEAD object — metadata-only fetch, tests req-rate ceiling.
    Stat,
}

impl S3Workload {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "read" | "get" | "get-only" => Ok(Self::Read),
            "mixed" | "warp" => Ok(Self::Mixed),
            "write" | "put" | "put-only" => Ok(Self::Write),
            "stat" | "head" => Ok(Self::Stat),
            other => Err(anyhow!(
                "unknown workload '{other}'; expected read | mixed | write | stat"
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub object_size: usize,
    pub seed_objects: usize,
    pub workload: S3Workload,
    pub timeout: Duration,
}

pub fn build_client(cfg: &S3Config) -> Result<Client> {
    let creds = Credentials::new(
        cfg.access_key.clone(),
        cfg.secret_key.clone(),
        None,
        None,
        "databench",
    );
    let region = Region::new(cfg.region.clone());
    let s3_config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(region)
        .credentials_provider(creds)
        .endpoint_url(cfg.endpoint.clone())
        .force_path_style(true)
        .build();
    Ok(Client::from_conf(s3_config))
}

pub async fn create_bucket(client: &Client, bucket: &str) -> Result<()> {
    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .with_context(|| format!("CreateBucket {bucket}"))?;
    Ok(())
}

pub async fn populate(
    client: &Client,
    bucket: &str,
    n: usize,
    payload: Bytes,
) -> Result<()> {
    for i in 0..n {
        let key = format!("obj{i}");
        client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(ByteStream::from(payload.clone()))
            .send()
            .await
            .with_context(|| format!("seed PUT {key}"))?;
    }
    Ok(())
}

pub async fn cleanup(client: &Client, bucket: &str) -> Result<u64> {
    if !bucket.starts_with("databench-") {
        return Err(anyhow!(
            "refusing to drop unexpected bucket '{bucket}' — \
             only buckets starting with `databench-` are auto-cleaned"
        ));
    }
    let mut continuation: Option<String> = None;
    let mut total = 0u64;
    loop {
        let mut req = client
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(1000);
        if let Some(c) = &continuation {
            req = req.continuation_token(c.clone());
        }
        let resp = req.send().await.context("ListObjectsV2")?;
        let objects = resp.contents();
        if !objects.is_empty() {
            let mut idents: Vec<ObjectIdentifier> = Vec::with_capacity(objects.len());
            for obj in objects {
                if let Some(k) = obj.key() {
                    if let Ok(id) = ObjectIdentifier::builder().key(k).build() {
                        idents.push(id);
                    }
                }
            }
            if !idents.is_empty() {
                let n = idents.len() as u64;
                if let Ok(del) = Delete::builder().set_objects(Some(idents)).build() {
                    client
                        .delete_objects()
                        .bucket(bucket)
                        .delete(del)
                        .send()
                        .await
                        .context("DeleteObjects")?;
                    total += n;
                }
            }
        }
        if !resp.is_truncated().unwrap_or(false) {
            break;
        }
        continuation = resp.next_continuation_token().map(String::from);
        if continuation.is_none() {
            break;
        }
    }
    client
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .context("DeleteBucket")?;
    Ok(total)
}

pub fn spawn_workers(
    client: Client,
    cfg: Arc<S3Config>,
    payload: Bytes,
    connections: usize,
    h: WorkerHandles,
) -> Vec<JoinHandle<WorkerReport>> {
    (0..connections)
        .map(|i| {
            let client = client.clone();
            let cfg = cfg.clone();
            let payload = payload.clone();
            let h = h.clone();
            tokio::spawn(async move { run_worker(client, cfg, payload, h, i as u64).await })
        })
        .collect()
}

#[derive(Copy, Clone)]
enum Op {
    Get,
    Put,
    Stat,
    Delete,
}

impl Op {
    fn label(self) -> &'static str {
        match self {
            Op::Get => "GET",
            Op::Put => "PUT",
            Op::Stat => "STAT",
            Op::Delete => "DELETE",
        }
    }
}

fn pick_op(workload: S3Workload, rng: &mut SmallRng) -> Op {
    match workload {
        S3Workload::Read => Op::Get,
        S3Workload::Write => Op::Put,
        S3Workload::Stat => Op::Stat,
        S3Workload::Mixed => {
            // warp default: 45 / 30 / 15 / 10
            let r: u32 = rng.gen_range(0..100);
            if r < 45 {
                Op::Get
            } else if r < 75 {
                Op::Stat
            } else if r < 90 {
                Op::Put
            } else {
                Op::Delete
            }
        }
    }
}

async fn run_worker(
    client: Client,
    cfg: Arc<S3Config>,
    payload: Bytes,
    h: WorkerHandles,
    worker_idx: u64,
) -> WorkerReport {
    let mut report = WorkerReport::new();
    let mut rng = SmallRng::seed_from_u64(0x53CA_FEBA_BE ^ worker_idx);

    while !h.stop.load(Ordering::Relaxed) {
        let measuring = h.measuring();
        if measuring && try_reserve_budget(&h.remaining).is_none() {
            break;
        }
        h.rate_gate().await;

        let op = pick_op(cfg.workload, &mut rng);
        let key_idx = rng.gen_range(0..cfg.seed_objects.max(1));
        let key = format!("obj{key_idx}");
        let start = Instant::now();
        let outcome: Result<(&'static str, u64)> = match op {
            Op::Get => match do_get(&client, &cfg.bucket, &key, cfg.timeout).await {
                Ok(Outcome::Hit(n)) => Ok(("GET", n)),
                Ok(Outcome::Miss) => Ok(("GET_404", 0)),
                Err(e) => Err(e),
            },
            Op::Put => do_put(&client, &cfg.bucket, &key, payload.clone(), cfg.timeout)
                .await
                .map(|_| ("PUT", payload.len() as u64)),
            Op::Stat => match do_stat(&client, &cfg.bucket, &key, cfg.timeout).await {
                Ok(Outcome::Hit(_)) => Ok(("STAT", 0)),
                Ok(Outcome::Miss) => Ok(("STAT_404", 0)),
                Err(e) => Err(e),
            },
            Op::Delete => do_delete(&client, &cfg.bucket, &key, cfg.timeout)
                .await
                .map(|_| ("DELETE", 0)),
        };
        let us = start.elapsed().as_micros() as u64;
        if !measuring {
            continue;
        }
        match outcome {
            Ok((label, bytes)) => {
                report.record_op(label, us);
                h.live.requests.fetch_add(1, Ordering::Relaxed);
                h.live.bytes.fetch_add(bytes, Ordering::Relaxed);
                h.live.record_request_phases(us, 0);
            }
            Err(e) => {
                report.record_error(&h.live, &format!("s3 {}: {}", op.label(), short_err(&e)));
            }
        }
    }
    report
}

fn short_err(e: &anyhow::Error) -> String {
    let s = e.to_string();
    // SDK error chains can be very long; keep the head line.
    s.lines().next().unwrap_or("error").to_string()
}

/// Result of a GET / STAT in mixed mode where keys come and go.
/// `Hit(bytes)` is a real read; `Miss` is a 404 from a DELETE earlier in
/// the run — still a valid response from the server, just not an error.
enum Outcome {
    Hit(u64),
    Miss,
}

async fn do_get(
    client: &Client,
    bucket: &str,
    key: &str,
    timeout: Duration,
) -> Result<Outcome> {
    let resp = match tokio::time::timeout(
        timeout,
        client.get_object().bucket(bucket).key(key).send(),
    )
    .await
    {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => match e.into_service_error() {
            GetObjectError::NoSuchKey(_) => return Ok(Outcome::Miss),
            other => return Err(anyhow::Error::new(other).context("GET")),
        },
        Err(_) => return Err(anyhow!("timeout")),
    };
    let body = resp.body.collect().await.context("GET body")?;
    Ok(Outcome::Hit(body.into_bytes().len() as u64))
}

async fn do_put(
    client: &Client,
    bucket: &str,
    key: &str,
    payload: Bytes,
    timeout: Duration,
) -> Result<()> {
    match tokio::time::timeout(
        timeout,
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(payload))
            .send(),
    )
    .await
    {
        Ok(r) => {
            r.context("PUT")?;
            Ok(())
        }
        Err(_) => Err(anyhow!("timeout")),
    }
}

async fn do_stat(client: &Client, bucket: &str, key: &str, timeout: Duration) -> Result<Outcome> {
    match tokio::time::timeout(
        timeout,
        client.head_object().bucket(bucket).key(key).send(),
    )
    .await
    {
        Ok(Ok(_)) => Ok(Outcome::Hit(0)),
        Ok(Err(e)) => match e.into_service_error() {
            HeadObjectError::NotFound(_) => Ok(Outcome::Miss),
            other => Err(anyhow::Error::new(other).context("STAT")),
        },
        Err(_) => Err(anyhow!("timeout")),
    }
}

async fn do_delete(client: &Client, bucket: &str, key: &str, timeout: Duration) -> Result<()> {
    match tokio::time::timeout(
        timeout,
        client.delete_object().bucket(bucket).key(key).send(),
    )
    .await
    {
        Ok(r) => {
            r.context("DELETE")?;
            Ok(())
        }
        Err(_) => Err(anyhow!("timeout")),
    }
}
