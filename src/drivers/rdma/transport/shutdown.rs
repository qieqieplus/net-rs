//! Graceful shutdown and QP drain logic.
//!
//! This module implements proper RDMA connection teardown following the reference
//! library's approach:
//!
//! 1. Send CLOSE message via immediate data
//! 2. Wait for peer's CLOSE or local CLOSE completion
//! 3. Drain all outstanding CQ entries
//! 4. On timeout, transition QP to Error state to flush remaining WRs

use super::protocol::{is_close_imm, IMM_KIND_CLOSE};
use crate::drivers::rdma::poller::{Completion, RdmaPoller};
use sideway::ibverbs::completion::WorkCompletionStatus;
use sideway::ibverbs::queue_pair::{
    GenericQueuePair, PostSendGuard as _, QueuePair, QueuePairAttribute, QueuePairState,
    WorkRequestFlags,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{debug, warn};

/// Configuration for shutdown behavior.
#[derive(Debug, Clone)]
pub struct ShutdownConfig {
    /// Total time to wait for graceful shutdown before forcing.
    pub timeout: Duration,
    /// Interval between drain polls.
    pub poll_interval: Duration,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(500),
            poll_interval: Duration::from_millis(10),
        }
    }
}

/// Result of a shutdown attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownResult {
    /// Both sides completed graceful close handshake.
    Graceful,
    /// Timed out waiting for peer; QP forced to Error state.
    ForcedError,
    /// Shutdown was already in progress.
    AlreadyShuttingDown,
}

/// Performs graceful shutdown of the RDMA transport.
///
/// This function:
/// 1. Sends a CLOSE message to the peer
/// 2. Waits for peer's CLOSE (via completion with immediate data)
/// 3. Drains outstanding completions from the CQ
/// 4. Forces QP to Error state if timeout is reached
pub async fn graceful_shutdown(
    qp: &Arc<Mutex<GenericQueuePair>>,
    next_wr_id: &Arc<AtomicU64>,
    poller: &Arc<RdmaPoller>,
    shutdown_initiated: &Arc<AtomicBool>,
    shutdown_received: &Arc<AtomicBool>,
    config: &ShutdownConfig,
) -> ShutdownResult {
    // Mark shutdown as initiated (SeqCst for visibility across threads)
    if shutdown_initiated.swap(true, Ordering::SeqCst) {
        return ShutdownResult::AlreadyShuttingDown;
    }

    debug!("initiating graceful shutdown");

    // Step 1: Send CLOSE message
    let close_wr_id = next_wr_id.fetch_add(1, Ordering::Relaxed);
    if let Err(e) = send_close_message(qp, close_wr_id).await {
        warn!("failed to send CLOSE message: {}", e);
        // Continue with drain anyway - peer may still receive it
    }

    // Step 2: Wait for peer CLOSE or our CLOSE completion
    let deadline = Instant::now() + config.timeout;

    loop {
        // Check if peer sent CLOSE
        if shutdown_received.load(Ordering::SeqCst) {
            debug!("received CLOSE from peer");
            break;
        }

        // Check if our CLOSE completed (indicates peer received it)
        if let Some(completion) = poller.take_completion(close_wr_id) {
            if completion.status == WorkCompletionStatus::Success {
                debug!("CLOSE message sent successfully");
                // Give peer a moment to process and respond
                tokio::time::sleep(config.poll_interval).await;
            } else {
                warn!("CLOSE message failed: {:?}", completion.status);
            }
        }

        // Check timeout
        if Instant::now() >= deadline {
            debug!("shutdown timeout reached, forcing QP to Error state");
            force_qp_error(qp).await;
            return ShutdownResult::ForcedError;
        }

        tokio::time::sleep(config.poll_interval).await;
    }

    // Step 3: Drain remaining completions (brief wait)
    let drain_deadline = Instant::now() + Duration::from_millis(50);
    while Instant::now() < drain_deadline {
        // The poller thread will consume completions; just wait briefly
        tokio::time::sleep(config.poll_interval).await;
    }

    debug!("graceful shutdown complete");
    ShutdownResult::Graceful
}

/// Send a CLOSE message using immediate data.
async fn send_close_message(
    qp: &Arc<Mutex<GenericQueuePair>>,
    wr_id: u64,
) -> Result<(), String> {
    let mut qp_guard = qp.lock().await;
    let mut guard = qp_guard.start_post_send();
    let wr = guard.construct_wr(wr_id, WorkRequestFlags::Signaled);
    wr.setup_send_imm(IMM_KIND_CLOSE);
    guard.post().map_err(|e| e.to_string())?;
    debug!("sent CLOSE message (wr_id={})", wr_id);
    Ok(())
}

/// Force QP to Error state to flush all outstanding work requests.
///
/// This is called when graceful shutdown times out. It causes all outstanding
/// WRs to complete with error status, which the poller will handle.
async fn force_qp_error(qp: &Arc<Mutex<GenericQueuePair>>) {
    let mut qp_guard = qp.lock().await;

    // Check current state before modifying
    let current_state = qp_guard.state();
    if matches!(
        current_state,
        QueuePairState::Error | QueuePairState::Reset
    ) {
        debug!("QP already in {:?} state, no transition needed", current_state);
        return;
    }

    let mut attr = QueuePairAttribute::new();
    attr.setup_state(QueuePairState::Error);

    match qp_guard.modify(&attr) {
        Ok(()) => {
            debug!("transitioned QP to Error state");
        }
        Err(e) => {
            // This can fail if QP is already in an error-like state
            warn!("failed to transition QP to Error state: {}", e);
        }
    }
}

/// Check if a completion indicates a CLOSE message from peer.
#[inline]
#[allow(dead_code)]
pub fn is_close_completion(completion: &Completion) -> bool {
    if let Some(imm_data) = completion.imm_data {
        is_close_imm(imm_data)
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_config_default() {
        let config = ShutdownConfig::default();
        assert_eq!(config.timeout, Duration::from_millis(500));
        assert_eq!(config.poll_interval, Duration::from_millis(10));
    }
}
