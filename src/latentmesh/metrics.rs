use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Default)]
pub struct LatentMeshMetrics {
    pub messages_sent: AtomicU64,
    pub messages_received: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub connect_attempts: AtomicU64,
    pub connect_successes: AtomicU64,
    pub connect_failures: AtomicU64,
    pub session_duration_ms: AtomicU64,
    pub last_latency_ns: AtomicU64,
    pub min_latency_ns: AtomicU64,
    pub max_latency_ns: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct LatentMeshMetricsSnapshot {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub connect_attempts: u64,
    pub connect_successes: u64,
    pub connect_failures: u64,
    pub session_duration_ms: u64,
    pub last_latency_ns: u64,
    pub min_latency_ns: u64,
    pub max_latency_ns: u64,
}

impl LatentMeshMetrics {
    pub fn new() -> Self {
        Self {
            min_latency_ns: AtomicU64::new(u64::MAX),
            ..Default::default()
        }
    }

    pub fn record_sent(&self, bytes: usize) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_received(&self, bytes: usize) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_connect_attempt(&self) {
        self.connect_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_connect_success(&self) {
        self.connect_successes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_connect_failure(&self) {
        self.connect_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_latency(&self, duration_ns: u64) {
        self.last_latency_ns.store(duration_ns, Ordering::Relaxed);
        update_min(&self.min_latency_ns, duration_ns);
        update_max(&self.max_latency_ns, duration_ns);
    }

    pub fn record_session_duration(&self, duration_ms: u64) {
        self.session_duration_ms
            .fetch_add(duration_ms, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> LatentMeshMetricsSnapshot {
        LatentMeshMetricsSnapshot {
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            connect_attempts: self.connect_attempts.load(Ordering::Relaxed),
            connect_successes: self.connect_successes.load(Ordering::Relaxed),
            connect_failures: self.connect_failures.load(Ordering::Relaxed),
            session_duration_ms: self.session_duration_ms.load(Ordering::Relaxed),
            last_latency_ns: self.last_latency_ns.load(Ordering::Relaxed),
            min_latency_ns: self.min_latency_ns.load(Ordering::Relaxed),
            max_latency_ns: self.max_latency_ns.load(Ordering::Relaxed),
        }
    }
}

pub struct ScopedTimer {
    start: Instant,
    metrics: Arc<LatentMeshMetrics>,
}

impl ScopedTimer {
    pub fn new(metrics: Arc<LatentMeshMetrics>) -> Self {
        Self {
            start: Instant::now(),
            metrics,
        }
    }
}

impl Drop for ScopedTimer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_nanos() as u64;
        self.metrics.record_latency(elapsed);
    }
}

fn update_min(counter: &AtomicU64, value: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    while value < current {
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

fn update_max(counter: &AtomicU64, value: u64) {
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
