use sideway::ibverbs::completion::{GenericCompletionQueue, PollCompletionQueueError, WorkCompletionStatus};
use sideway::ibverbs::completion::WorkCompletionOperationType;
use crossbeam::channel::{unbounded, Sender};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Waker;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;

#[derive(Debug)]
enum PollerCmd {
    Register { wr_id: u64, waker: Waker },
    Cancel { wr_id: u64 },
}

#[derive(Debug, Clone, Copy)]
pub struct Completion {
    pub status: WorkCompletionStatus,
    pub opcode: WorkCompletionOperationType,
    pub byte_len: u32,
    /// Immediate data associated with this completion (valid for *WithImmediate opcodes).
    pub imm_data: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
pub struct CompletionEvent {
    pub wr_id: u64,
    pub completion: Completion,
}

#[allow(dead_code)]
pub struct RdmaPoller {
    cq: GenericCompletionQueue,
    cmd_tx: Sender<PollerCmd>,
    completions: Arc<DashMap<u64, Completion>>,
    shutdown: Arc<AtomicBool>,
    recv_tx: Option<UnboundedSender<CompletionEvent>>,
    join_handle: Option<std::thread::JoinHandle<()>>,
}

unsafe impl Send for RdmaPoller {}
unsafe impl Sync for RdmaPoller {}

impl RdmaPoller {
    pub fn new(cq: GenericCompletionQueue) -> Self {
        Self::new_with_recv(cq, None)
    }

    pub fn new_with_recv(cq: GenericCompletionQueue, recv_tx: Option<UnboundedSender<CompletionEvent>>) -> Self {
        let (cmd_tx, cmd_rx) = unbounded::<PollerCmd>();
        let completions = Arc::new(DashMap::<u64, Completion>::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        
        let cq_clone = cq.clone();
        let completions_clone = Arc::clone(&completions);
        let shutdown_clone = shutdown.clone();
        let recv_tx_clone = recv_tx.clone();

        let join_handle = thread::spawn(move || {
            let mut wakers: HashMap<u64, Waker> = HashMap::new();
            let mut canceled: HashSet<u64> = HashSet::new();
            let mut idle_count: u32 = 0;
            let mut last_err_log = Instant::now();
            let mut err_suppressed: u64 = 0;

            while !shutdown_clone.load(Ordering::Relaxed) {
                // Drain registration/cancellation commands (non-blocking).
                while let Ok(cmd) = cmd_rx.try_recv() {
                    match cmd {
                        PollerCmd::Register { wr_id, waker } => {
                            // Refresh the waker (it may change between polls).
                            wakers.insert(wr_id, waker);
                            // If we previously saw a cancel for this wr_id, drop it: wr_ids are
                            // expected to be unique for in-flight requests; repeated Register calls
                            // are normal as the future is polled.
                            canceled.remove(&wr_id);
                        }
                        PollerCmd::Cancel { wr_id } => {
                            wakers.remove(&wr_id);
                            // If the completion already arrived, drop it now and do NOT record the
                            // cancellation (there will be no future completion to consume it).
                            let had_completion = completions_clone.remove(&wr_id).is_some();
                            if !had_completion {
                                canceled.insert(wr_id);
                            }
                        }
                    }
                }

                // Poll CQ (Sideway's start_poll returns an iterator)
                match cq_clone.start_poll() {
                    Ok(poller) => {
                         let mut count = 0;
                         for completion in poller {
                             count += 1;
                             idle_count = 0;
                             let status = WorkCompletionStatus::from(completion.status());
                             let opcode = WorkCompletionOperationType::from(completion.opcode());
                             let byte_len = completion.byte_len();
                             let wr_id = completion.wr_id();

                             if status != WorkCompletionStatus::Success {
                                 // Avoid blocking I/O in the hot path; rate-limit error logging.
                                 if last_err_log.elapsed() >= Duration::from_secs(1) {
                                     if err_suppressed > 0 {
                                         error!("RDMA WC Error: suppressed {} errors in the last 1s", err_suppressed);
                                     }
                                     err_suppressed = 0;
                                     last_err_log = Instant::now();
                                 }
                                 if err_suppressed < 16 {
                                     error!("RDMA WC Error: status={:?} opcode={:?} wr_id={}", status, opcode, wr_id);
                                 } else {
                                     err_suppressed += 1;
                                 }
                             }

                             let imm_data = match opcode {
                                 WorkCompletionOperationType::ReceiveWithImmediate => Some(completion.imm_data()),
                                 _ => None,
                             };
                             let c = Completion {
                                 status,
                                 opcode,
                                 byte_len,
                                 imm_data,
                             };

                             // Fast-path receive completions to a dedicated consumer (if configured)
                             if matches!(opcode, WorkCompletionOperationType::Receive | WorkCompletionOperationType::ReceiveWithImmediate) {
                                 if let Some(tx) = &recv_tx_clone {
                                     let _ = tx.send(CompletionEvent { wr_id, completion: c });
                                 } else {
                                     completions_clone.insert(wr_id, c);
                                 }
                                 continue;
                             }

                             // If the waiter was cancelled, drop the completion and clear the mark.
                             if canceled.remove(&wr_id) {
                                 continue;
                             }

                             // Otherwise, store completion and wake any waiter.
                             completions_clone.insert(wr_id, c);
                             if let Some(waker) = wakers.remove(&wr_id) {
                                 waker.wake();
                             }
                         }
                         if count == 0 {
                             idle_count = idle_count.saturating_add(1);
                         }
                    }
                    Err(PollCompletionQueueError::CompletionQueueEmpty) => {
                        idle_count = idle_count.saturating_add(1);
                    }
                    Err(e) => {
                        // Not expected in the hot path; still avoid stdout/stderr.
                        error!("Failed to start poll: {:?}", e);
                        idle_count = idle_count.saturating_add(1);
                    }
                }

                // Adaptive idle strategy: busy-poll briefly, then yield, then sleep.
                if idle_count > 100 {
                    thread::sleep(Duration::from_micros(10));
                } else if idle_count > 10 {
                    thread::yield_now();
                }
            }
        });

        Self {
            cq,
            cmd_tx,
            completions,
            shutdown,
            recv_tx,
            join_handle: Some(join_handle),
        }
    }

    pub fn register(&self, wr_id: u64, waker: Waker) {
        // Best-effort; if poller thread is gone we just stop waking.
        let _ = self.cmd_tx.send(PollerCmd::Register { wr_id, waker });
    }

    /// Best-effort cancellation for an in-flight `wr_id`.
    ///
    /// This is primarily used to prevent unbounded growth if the waiting future is dropped
    /// (timeout/cancellation). It does **not** cancel the underlying RDMA operation.
    pub fn cancel(&self, wr_id: u64) {
        let _ = self.cmd_tx.send(PollerCmd::Cancel { wr_id });
    }

    pub fn take_completion(&self, wr_id: u64) -> Option<Completion> {
        self.completions.remove(&wr_id).map(|(_, c)| c)
    }
}

impl Drop for RdmaPoller {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }
}
