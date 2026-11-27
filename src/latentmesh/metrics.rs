use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Debug, Default)]
pub struct LatentMeshMetrics {
    proposals_received: AtomicU64,
    proposals_accepted: AtomicU64,
    proposals_rejected: AtomicU64,
    proposals_applied: AtomicU64,
    divergence_count: AtomicU64,
    sync_failures: AtomicU64,
    last_rtt_us: AtomicU64,
    min_rtt_us: AtomicU64,
    max_rtt_us: AtomicU64,
    sum_rtt_us: AtomicU64,
    rtt_samples: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LatentMeshMetricsSnapshot {
    pub proposals_received: u64,
    pub proposals_accepted: u64,
    pub proposals_rejected: u64,
    pub proposals_applied: u64,
    pub divergence_count: u64,
    pub sync_failures: u64,
    pub last_rtt: Option<Duration>,
    pub min_rtt: Option<Duration>,
    pub max_rtt: Option<Duration>,
    pub avg_rtt: Option<Duration>,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

impl LatentMeshMetrics {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            proposals_received: AtomicU64::new(0),
            proposals_accepted: AtomicU64::new(0),
            proposals_rejected: AtomicU64::new(0),
            proposals_applied: AtomicU64::new(0),
            divergence_count: AtomicU64::new(0),
            sync_failures: AtomicU64::new(0),
            last_rtt_us: AtomicU64::new(0),
            min_rtt_us: AtomicU64::new(0),
            max_rtt_us: AtomicU64::new(0),
            sum_rtt_us: AtomicU64::new(0),
            rtt_samples: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
        }
    }

    pub fn record_proposal_received(&self) {
        self.proposals_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_proposal_accepted(&self) {
        self.proposals_accepted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_proposal_rejected(&self) {
        self.proposals_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_proposal_applied(&self) {
        self.proposals_applied.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_divergence(&self) {
        self.divergence_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_sync_failure(&self) {
        self.sync_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_bytes_sent(&self, bytes: usize) {
        self.bytes_sent
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_bytes_received(&self, bytes: usize) {
        self.bytes_received
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_rtt(&self, rtt: Duration) {
        let us = rtt.as_micros().min(u64::MAX as u128) as u64;
        self.last_rtt_us.store(us, Ordering::Relaxed);
        self.sum_rtt_us.fetch_add(us, Ordering::Relaxed);
        self.rtt_samples.fetch_add(1, Ordering::Relaxed);

        update_atomic_min(&self.min_rtt_us, us);
        update_atomic_max(&self.max_rtt_us, us);
    }

    #[must_use]
    pub fn snapshot(&self) -> LatentMeshMetricsSnapshot {
        let samples = self.rtt_samples.load(Ordering::Relaxed);
        let sum_us = self.sum_rtt_us.load(Ordering::Relaxed);
        let last_us = self.last_rtt_us.load(Ordering::Relaxed);
        let min_us = self.min_rtt_us.load(Ordering::Relaxed);
        let max_us = self.max_rtt_us.load(Ordering::Relaxed);

        let (last_rtt, min_rtt, max_rtt, avg_rtt) = if samples == 0 {
            (None, None, None, None)
        } else {
            (
                Some(Duration::from_micros(last_us)),
                Some(Duration::from_micros(min_us)),
                Some(Duration::from_micros(max_us)),
                Some(Duration::from_micros(sum_us / samples)),
            )
        };

        LatentMeshMetricsSnapshot {
            proposals_received: self.proposals_received.load(Ordering::Relaxed),
            proposals_accepted: self.proposals_accepted.load(Ordering::Relaxed),
            proposals_rejected: self.proposals_rejected.load(Ordering::Relaxed),
            proposals_applied: self.proposals_applied.load(Ordering::Relaxed),
            divergence_count: self.divergence_count.load(Ordering::Relaxed),
            sync_failures: self.sync_failures.load(Ordering::Relaxed),
            last_rtt,
            min_rtt,
            max_rtt,
            avg_rtt,
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
        }
    }
}

fn update_atomic_min(counter: &AtomicU64, value: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    while current == 0 || value < current {
        match counter.compare_exchange_weak(
            current,
            value,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

fn update_atomic_max(counter: &AtomicU64, value: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    while value > current {
        match counter.compare_exchange_weak(
            current,
            value,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}
