use std::sync::Arc;
use tokio::sync::Semaphore;

/// Credit-based flow controller for RDMA transport.
///
/// Prevents sender from overwhelming receiver by limiting outstanding sends.
/// Credits are consumed on send and restored when receiver ACKs via immediate data.
#[derive(Clone, Debug)]
pub struct FlowController {
    /// Maximum credits (initial window size)
    max_credits: usize,
    /// Semaphore for async credit acquisition
    semaphore: Arc<Semaphore>,
}

impl FlowController {
    /// Create a new flow controller with the given maximum credits.
    pub fn new(max_credits: usize) -> Self {
        Self {
            max_credits,
            semaphore: Arc::new(Semaphore::new(max_credits)),
        }
    }

    /// Acquire a send credit, blocking if none available.
    ///
    /// Returns a guard that can be used to track the send.
    /// Credits are NOT automatically returned on drop - they must be
    /// explicitly restored via ACK messages.
    pub async fn acquire(&self) -> FlowControlGuard {
        let permit = self.semaphore.acquire().await.expect("Semaphore closed");
        permit.forget(); // Don't auto-return on drop
        FlowControlGuard { _guard: () }
    }

    /// Try to acquire a credit without blocking.
    #[allow(dead_code)]
    pub fn try_acquire(&self) -> Option<FlowControlGuard> {
        self.semaphore.try_acquire().ok().map(|permit| {
            permit.forget();
            FlowControlGuard { _guard: () }
        })
    }

    /// Restore credits (called when receiving ACK from peer).
    pub fn release(&self, count: usize) {
        if count == 0 {
            return;
        }
        let available = self.semaphore.available_permits();
        if available >= self.max_credits {
            return;
        }
        let remaining = self.max_credits - available;
        debug_assert!(
            count <= remaining,
            "flow control release would exceed max_credits (count={}, available={}, max_credits={})",
            count,
            available,
            self.max_credits
        );
        self.semaphore.add_permits(count.min(remaining));
    }

    /// Get current available credits (approximate).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Get maximum credits.
    #[allow(dead_code)]
    pub fn max_credits(&self) -> usize {
        self.max_credits
    }
}

/// Guard representing an acquired send credit.
///
/// Credits are NOT released on drop - they are released when
/// the peer ACKs via immediate data.
#[must_use = "Dropping this guard does not return the credit. Ensure the message is sent or explicitly handled."]
pub struct FlowControlGuard {
    _guard: (),
}

impl Drop for FlowControlGuard {
    fn drop(&mut self) {
        // Intentionally empty: credits are restored via ACK, not RAII.
        // Note: if this guard is dropped after `permit.forget()` but before the send happens
        // (e.g. due to cancellation/panic), that credit is effectively leaked until reconnect.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_flow_control_basic() {
        let fc = FlowController::new(2);

        assert_eq!(fc.available(), 2);

        let _guard1 = fc.acquire().await;
        assert_eq!(fc.available(), 1);

        let _guard2 = fc.acquire().await;
        assert_eq!(fc.available(), 0);

        // Release credits
        fc.release(2);
        assert_eq!(fc.available(), 2);
    }

    #[tokio::test]
    async fn test_flow_control_blocking() {
        let fc = FlowController::new(1);
        let fc_clone = fc.clone();

        let _guard = fc.acquire().await;
        assert_eq!(fc.available(), 0);

        // Spawn task that will block on acquire
        let handle = tokio::spawn(async move {
            let _ = fc_clone.acquire().await;
        });

        // Release credit to unblock
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        fc.release(1);

        // Task should complete
        tokio::time::timeout(tokio::time::Duration::from_millis(100), handle)
            .await
            .expect("Task should complete")
            .unwrap();
    }
}
