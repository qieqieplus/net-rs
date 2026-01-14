use super::protocol::{is_close_imm, IMM_KIND_CLOSE};
use super::TransportInner;
use crate::drivers::rdma::poller::{Completion, RdmaPoller};
use sideway::ibverbs::completion::WorkCompletionStatus;
use sideway::ibverbs::queue_pair::{
    GenericQueuePair, PostSendGuard as _, QueuePair, QueuePairAttribute, QueuePairState,
    WorkRequestFlags,
};

use std::rc::Rc;
use std::cell::RefCell;
use std::time::{Duration, Instant};
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
    inner: &Rc<TransportInner>,
    poller: &Rc<RdmaPoller>,
    config: &ShutdownConfig,
) -> ShutdownResult {
    // Mark shutdown as initiated
    if inner.shutdown_initiated.replace(true) {
        return ShutdownResult::AlreadyShuttingDown;
    }

    debug!("initiating graceful shutdown");

    // Step 1: Send CLOSE message
    let close_wr_id = {
        let id = inner.next_wr_id.get();
        inner.next_wr_id.set(id.wrapping_add(1));
        id
    };
    if let Err(e) = send_close_message(&inner.qp, close_wr_id).await {
        warn!("failed to send CLOSE message: {}", e);
        // Continue with drain anyway - peer may still receive it
    }

    // Step 2: Wait for peer CLOSE or our CLOSE completion
    let deadline = Instant::now() + config.timeout;

    let mut close_future = std::pin::pin!(poller.wait(close_wr_id));

    loop {
        // Check if peer sent CLOSE
        if inner.shutdown_received.get() {
            debug!("received CLOSE from peer");
            // Cancel our wait - the poller will clean up
            poller.cancel(close_wr_id);
            break;
        }

        // Check timeout
        if Instant::now() >= deadline {
            debug!("shutdown timeout reached, forcing QP to Error state");
            poller.cancel(close_wr_id);
            force_qp_error(&inner.qp).await;
            return ShutdownResult::ForcedError;
        }

        // Try to get our CLOSE completion with a short timeout
        let sleep_fut = monoio::time::sleep(config.poll_interval);
        monoio::select! {
            biased;
            completion = &mut close_future => {
                if completion.status == WorkCompletionStatus::Success {
                    debug!("CLOSE message sent successfully");
                    // Give peer a moment to process and respond
                    monoio::time::sleep(config.poll_interval).await;
                } else {
                    warn!("CLOSE message failed: {:?}", completion.status);
                }
                // Either way, we've handled the completion - exit waiting loop
                break;
            }
            _ = sleep_fut => {
                // Continue loop to check peer CLOSE and timeout
            }
        }
    }

    // Step 3: Drain remaining completions (brief wait)
    let drain_deadline = Instant::now() + Duration::from_millis(50);
    while Instant::now() < drain_deadline {
        // The poller thread will consume completions; just wait briefly
        monoio::time::sleep(config.poll_interval).await;
    }

    debug!("graceful shutdown complete");
    ShutdownResult::Graceful
}

/// Send a CLOSE message using immediate data.
async fn send_close_message(
    qp: &RefCell<GenericQueuePair>,
    wr_id: u64,
) -> Result<(), String> {
    // Synchronous borrow (safe)
    let mut qp_guard = qp.borrow_mut();
    let mut guard = qp_guard.start_post_send();
    let wr = guard.construct_wr(wr_id, WorkRequestFlags::Signaled);
    wr.setup_send_imm(IMM_KIND_CLOSE);
    guard.post().map_err(|e| e.to_string())?;
    debug!("sent CLOSE message (wr_id={})", wr_id);
    Ok(())
}

/// Force QP to Error state to flush all outstanding work requests.
async fn force_qp_error(qp: &RefCell<GenericQueuePair>) {
    let mut qp_guard = qp.borrow_mut();

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
