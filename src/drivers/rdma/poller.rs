use sideway::ibverbs::completion::{GenericCompletionQueue, PollCompletionQueueError, WorkCompletionStatus};
use sideway::ibverbs::completion::WorkCompletionOperationType;
use crossbeam::channel::{unbounded, Sender};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Waker;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone, Copy)]
pub struct Completion {
    pub status: WorkCompletionStatus,
    pub opcode: WorkCompletionOperationType,
    pub byte_len: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct CompletionEvent {
    pub wr_id: u64,
    pub completion: Completion,
}

#[allow(dead_code)]
pub struct RdmaPoller {
    cq: GenericCompletionQueue,
    waker_tx: Sender<(u64, Waker)>,
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
        let (waker_tx, waker_rx) = unbounded::<(u64, Waker)>();
        let completions = Arc::new(DashMap::<u64, Completion>::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        
        let cq_clone = cq.clone();
        let completions_clone = Arc::clone(&completions);
        let shutdown_clone = shutdown.clone();
        let recv_tx_clone = recv_tx.clone();

        let join_handle = thread::spawn(move || {
            let mut wakers: HashMap<u64, Waker> = HashMap::new();
            let mut idle_count: u32 = 0;

            while !shutdown_clone.load(Ordering::Relaxed) {
                // Drain waker registrations (non-blocking)
                while let Ok((wr_id, waker)) = waker_rx.try_recv() {
                    wakers.insert(wr_id, waker);
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
                                 eprintln!("RDMA WC Error: status={:?} opcode={:?} wr_id={}", status, opcode, wr_id);
                             }

                             let c = Completion { status, opcode, byte_len };

                             // Fast-path receive completions to a dedicated consumer (if configured)
                             if matches!(opcode, WorkCompletionOperationType::Receive | WorkCompletionOperationType::ReceiveWithImmediate) {
                                 if let Some(tx) = &recv_tx_clone {
                                     let _ = tx.send(CompletionEvent { wr_id, completion: c });
                                 } else {
                                     completions_clone.insert(wr_id, c);
                                 }
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
                        eprintln!("Failed to start poll: {:?}", e);
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
            waker_tx,
            completions,
            shutdown,
            recv_tx,
            join_handle: Some(join_handle),
        }
    }

    pub fn register(&self, wr_id: u64, waker: Waker) {
        // Best-effort; if poller thread is gone we just stop waking.
        let _ = self.waker_tx.send((wr_id, waker));
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
