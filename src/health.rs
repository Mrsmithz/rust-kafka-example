use std::{
    sync::{
        Mutex,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use tokio::time::Instant;

const STARTING: u8 = 0;
const RUNNING: u8 = 1;
const DRAINING: u8 = 2;
const FAILED: u8 = 3;

#[derive(Clone)]
pub(crate) struct Health {
    state: std::sync::Arc<AtomicU8>,
    broker_last_ok: std::sync::Arc<Mutex<Option<Instant>>>,
    stale_after: Duration,
}

impl Health {
    pub(crate) fn new(stale_after: Duration) -> Self {
        Self {
            state: std::sync::Arc::new(AtomicU8::new(STARTING)),
            broker_last_ok: std::sync::Arc::new(Mutex::new(None)),
            stale_after,
        }
    }

    pub(crate) fn running(&self) {
        let _ = self
            .state
            .compare_exchange(STARTING, RUNNING, Ordering::AcqRel, Ordering::Acquire);
    }

    pub(crate) fn draining(&self) {
        let _ =
            self.state
                .compare_exchange(STARTING, DRAINING, Ordering::AcqRel, Ordering::Acquire);
        let _ = self
            .state
            .compare_exchange(RUNNING, DRAINING, Ordering::AcqRel, Ordering::Acquire);
    }

    pub(crate) fn fail(&self) {
        self.state.store(FAILED, Ordering::Release);
    }

    pub(crate) fn broker_ok(&self) {
        if let Ok(mut last_ok) = self.broker_last_ok.lock() {
            *last_ok = Some(Instant::now());
        }
    }

    pub(crate) fn broker_unavailable(&self) {
        if let Ok(mut last_ok) = self.broker_last_ok.lock() {
            *last_ok = None;
        }
    }

    pub(crate) fn ready(&self) -> bool {
        if self.state.load(Ordering::Acquire) != RUNNING {
            return false;
        }
        self.broker_last_ok
            .lock()
            .ok()
            .and_then(|last_ok| *last_ok)
            .is_some_and(|last_ok| last_ok.elapsed() <= self.stale_after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn readiness_requires_running_and_a_fresh_broker_probe() {
        let health = Health::new(Duration::from_secs(5));
        assert!(!health.ready());
        health.broker_ok();
        assert!(!health.ready());
        health.running();
        assert!(health.ready());
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(!health.ready());
    }

    #[test]
    fn lifecycle_transitions_never_revive_draining_or_failed_state() {
        let draining = Health::new(Duration::from_secs(1));
        draining.running();
        draining.draining();
        draining.running();
        draining.broker_ok();
        assert!(!draining.ready());

        let failed = Health::new(Duration::from_secs(1));
        failed.fail();
        failed.running();
        failed.broker_ok();
        assert!(!failed.ready());
    }
}
