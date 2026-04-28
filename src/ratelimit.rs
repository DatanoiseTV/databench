//! Shared QPS cap. A leaky-bucket gate every worker passes through before
//! each probe — no buffer, no burst, just a steady "next probe is allowed
//! at time T" target advanced by `1 / qps` on each acquire.
//!
//! The mutex is only held to read+update the next-allowed timestamp; the
//! actual sleep happens after the lock is released, so contention stays
//! tiny even at very high concurrency.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

pub struct RateLimiter {
    interval: Duration,
    next_at: Mutex<Instant>,
}

impl RateLimiter {
    pub fn from_qps(qps: f64) -> Option<Arc<Self>> {
        if !qps.is_finite() || qps <= 0.0 {
            return None;
        }
        let interval = Duration::from_secs_f64(1.0 / qps);
        Some(Arc::new(Self {
            interval,
            next_at: Mutex::new(Instant::now()),
        }))
    }

    pub async fn acquire(&self) {
        let target = {
            let mut next = self.next_at.lock();
            let now = Instant::now();
            let t = (*next).max(now);
            *next = t + self.interval;
            t
        };
        tokio::time::sleep_until(target.into()).await;
    }
}
