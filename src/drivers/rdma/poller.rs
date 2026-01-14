//! Integrated RDMA Completion Queue Poller.
//!
//! This module provides an async-friendly CQ poller that integrates with
//! Monoio's io_uring event loop. Instead of using a dedicated polling thread,
//! it uses a `CompletionChannel` FD registered with io_uring for event notification.
//!
//! # Flow
//! 1. Create CQ with CompletionChannel
//! 2. Arm CQ for solicited notifications via `ibv_req_notify_cq`
//! 3. Wait for channel FD to be readable (io_uring)
//! 4. Read events via `ibv_get_cq_event` / `ibv_ack_cq_events`
//! 5. Poll CQ work completions
//! 6. Re-arm and repeat

use std::cell::RefCell;
use rustc_hash::FxHashMap;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::ptr;
use std::rc::Rc;
use std::sync::Arc;

use sideway::ibverbs::completion::{
    CompletionChannel, CompletionQueue, GenericCompletionQueue, PollCompletionQueueError,
    WorkCompletionOperationType, WorkCompletionStatus,
};
use tracing::{debug, error};

/// Completion result from an RDMA operation.
#[derive(Debug, Clone, Copy)]
pub struct Completion {
    pub status: WorkCompletionStatus,
    pub opcode: WorkCompletionOperationType,
    pub byte_len: u32,
    pub imm_data: Option<u32>,
}

/// A completion event with its work request ID.
#[derive(Debug, Clone, Copy)]
pub struct CompletionEvent {
    pub wr_id: u64,
    pub completion: Completion,
}

/// Pending wait state for an RDMA operation.
struct PendingWait {
    waker: Option<std::task::Waker>,
    completion: Option<Completion>,
}

/// Integrated RDMA Completion Queue Poller.
///
/// Uses io_uring to wait for CQ events, eliminating the need for a dedicated
/// polling thread. All RDMA completions are processed on the Monoio reactor thread.
pub struct RdmaPoller {
    /// The completion queue being polled.
    cq: GenericCompletionQueue,
    /// Completion channel for event notifications.
    channel: Arc<CompletionChannel>,
    /// Pending waits indexed by work request ID.
    pending: Rc<RefCell<FxHashMap<u64, PendingWait>>>,
    /// Error token bucket for rate-limited logging.
    err_tokens: RefCell<u32>,
}

impl RdmaPoller {
    /// Creates a new poller for the given CQ and channel.
    ///
    /// The CQ must have been created with this channel for event notification.
    pub fn new(cq: GenericCompletionQueue, channel: Arc<CompletionChannel>) -> Self {
        Self {
            cq,
            channel,
            pending: Rc::new(RefCell::new(FxHashMap::default())),
            err_tokens: RefCell::new(10),
        }
    }

    /// Get the raw FD of the completion channel for io_uring registration.
    pub fn channel_fd(&self) -> RawFd {
        self.channel.as_raw_fd()
    }

    /// Arm the CQ for solicited completions.
    ///
    /// Must be called before waiting on the channel FD.
    /// Returns an error if arming fails.
    pub fn arm(&self) -> io::Result<()> {
        // SAFETY: We have valid pointers to ibv_cq from the CQ.
        unsafe {
            let cq_ptr = match &self.cq {
                GenericCompletionQueue::Basic(bcq) => bcq.cq().as_ptr(),
                GenericCompletionQueue::Extended(ecq) => ecq.cq().as_ptr(),
            };
            // Request solicited notification (second arg = 0 means solicited only)
            let ret = rdma_mummy_sys::ibv_req_notify_cq(cq_ptr, 0);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }
        }
        Ok(())
    }

    /// Acknowledge CQ events after reading from the channel.
    ///
    /// Must be called after `ibv_get_cq_event` to prevent event queue overflow.
    pub fn ack_events(&self, count: u32) {
        if count == 0 {
            return;
        }
        // SAFETY: Valid CQ pointer.
        unsafe {
            let cq_ptr = match &self.cq {
                GenericCompletionQueue::Basic(bcq) => bcq.cq().as_ptr(),
                GenericCompletionQueue::Extended(ecq) => ecq.cq().as_ptr(),
            };
            rdma_mummy_sys::ibv_ack_cq_events(cq_ptr, count);
        }
    }

    /// Wait for and consume a CQ event from the completion channel.
    ///
    /// Returns `true` if an event was received, `false` on channel error.
    /// This is a blocking call in ibverbs but we wrap it with async FD polling.
    pub fn get_cq_event(&self) -> io::Result<bool> {
        // SAFETY: Valid channel and CQ pointers.
        unsafe {
            let channel_ptr = self.channel.comp_channel().as_ptr();
            let mut cq_ptr: *mut rdma_mummy_sys::ibv_cq = ptr::null_mut();
            let mut cq_ctx: *mut std::ffi::c_void = ptr::null_mut();

            let ret = rdma_mummy_sys::ibv_get_cq_event(channel_ptr, &mut cq_ptr, &mut cq_ctx);
            if ret != 0 {
                // EAGAIN means no event ready (shouldn't happen if FD was readable)
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::EAGAIN) {
                    return Ok(false);
                }
                return Err(err);
            }
            Ok(true)
        }
    }

    /// Poll the CQ and process all available work completions.
    ///
    /// This method accepts mutable references to event buffers to avoid allocations.
    /// Callers should clear the buffers before calling, and this method will append events.
    ///
    /// - recv_events: Receive completions that need payload handling
    /// - send_events: Send/RDMA completions for credit release
    ///
    /// Also wakes waiters for send/RDMA completions (for RDMA READ/WRITE futures).
    pub fn poll_cq(&self, recv_events: &mut Vec<CompletionEvent>, send_events: &mut Vec<CompletionEvent>) {
        use std::collections::hash_map::Entry;

        let mut err_tokens = self.err_tokens.borrow_mut();
        // Hoist borrow_mut outside the hot loop to eliminate repeated runtime borrow checks.
        let mut pending = self.pending.borrow_mut();

        loop {
            match self.cq.start_poll() {
                Ok(iter) => {
                    let mut found_any = false;
                    for wc in iter {
                        found_any = true;
                        let wr_id = wc.wr_id();
                        let status = WorkCompletionStatus::from(wc.status());
                        let opcode = WorkCompletionOperationType::from(wc.opcode());

                        // Rate-limited error logging
                        if status != WorkCompletionStatus::Success {
                            let is_flush =
                                matches!(status, WorkCompletionStatus::WorkRequestFlushedError);
                            if *err_tokens > 0 {
                                *err_tokens -= 1;
                                if is_flush {
                                    debug!("RDMA WC flushed (shutdown): id={} op={:?}", wr_id, opcode);
                                } else {
                                    error!(
                                        "RDMA WC Err: id={} status={:?} op={:?} vendor_err={}",
                                        wr_id, status, opcode, wc.vendor_err()
                                    );
                                }
                            }
                        }

                        let completion = Completion {
                            status,
                            opcode,
                            byte_len: wc.byte_len(),
                            imm_data: if matches!(
                                opcode,
                                WorkCompletionOperationType::ReceiveWithImmediate
                            ) {
                                Some(wc.imm_data())
                            } else {
                                None
                            },
                        };

                        // Recv completions are streamed, not awaited
                        if matches!(
                            opcode,
                            WorkCompletionOperationType::Receive
                                | WorkCompletionOperationType::ReceiveWithImmediate
                        ) {
                            recv_events.push(CompletionEvent { wr_id, completion });
                            continue;
                        }

                        // Send/RDMA completions: collect for credit release AND wake waiter
                        send_events.push(CompletionEvent { wr_id, completion });

                        // Use Entry API to avoid double hashing/lookup
                        match pending.entry(wr_id) {
                            Entry::Occupied(mut entry) => {
                                let pw = entry.get_mut();
                                pw.completion = Some(completion);
                                if let Some(waker) = pw.waker.take() {
                                    waker.wake();
                                }
                            }
                            Entry::Vacant(entry) => {
                                // Store for future waiter (missed wakeup case)
                                entry.insert(PendingWait {
                                    waker: None,
                                    completion: Some(completion),
                                });
                            }
                        }
                    }
                    if !found_any {
                        break;
                    }
                }
                Err(PollCompletionQueueError::CompletionQueueEmpty) => break,
                Err(e) => {
                    error!("RDMA CQ poll error: {:?}", e);
                    break;
                }
            }
        }

        // Refill error token bucket periodically
        if *err_tokens < 10 {
            *err_tokens = (*err_tokens).saturating_add(1);
        }
    }

    /// Creates a future that waits for a specific work request to complete.
    pub fn wait(&self, wr_id: u64) -> RdmaOpFuture {
        RdmaOpFuture {
            wr_id,
            pending: Rc::clone(&self.pending),
            registered: false,
        }
    }

    /// Cancel a pending wait.
    pub fn cancel(&self, wr_id: u64) {
        self.pending.borrow_mut().remove(&wr_id);
    }
}

/// Future that waits for an RDMA operation to complete.
pub struct RdmaOpFuture {
    wr_id: u64,
    pending: Rc<RefCell<FxHashMap<u64, PendingWait>>>,
    registered: bool,
}

impl std::future::Future for RdmaOpFuture {
    type Output = Completion;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let wr_id = self.wr_id;
        
        {
            let mut pending = self.pending.borrow_mut();

            // Check if completion already arrived
            if let Some(pw) = pending.get_mut(&wr_id) {
                if let Some(completion) = pw.completion.take() {
                    pending.remove(&wr_id);
                    return std::task::Poll::Ready(completion);
                }
                // Update waker
                pw.waker = Some(cx.waker().clone());
            } else {
                // Register interest
                pending.insert(
                    wr_id,
                    PendingWait {
                        waker: Some(cx.waker().clone()),
                        completion: None,
                    },
                );
            }
        } // `pending` dropped here
        
        self.registered = true;
        std::task::Poll::Pending
    }
}

impl Drop for RdmaOpFuture {
    fn drop(&mut self) {
        if self.registered {
            // Remove waker but keep completion if it exists (for cleanup)
            let mut pending = self.pending.borrow_mut();
            if let Some(pw) = pending.get_mut(&self.wr_id) {
                pw.waker = None;
                // If no completion, remove entry entirely
                if pw.completion.is_none() {
                    pending.remove(&self.wr_id);
                }
            }
        }
    }
}