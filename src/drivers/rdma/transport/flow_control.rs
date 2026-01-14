use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

#[derive(Clone, Debug)]
pub struct FlowController {
    inner: Rc<RefCell<Inner>>,
}

#[derive(Debug)]
struct Inner {
    available: usize,
    max_credits: usize,
    next_waiter_id: u64,
    waiters: VecDeque<Waiter>,
}

#[derive(Debug)]
struct Waiter {
    id: u64,
    needed: usize,
    waker: Waker,
}

impl Inner {
    fn ready_head_waker(&self) -> Option<Waker> {
        let head = self.waiters.front()?;
        if self.available >= head.needed {
            Some(head.waker.clone())
        } else {
            None
        }
    }

    fn remove_waiter(&mut self, id: u64) {
        if let Some(front) = self.waiters.front() {
            if front.id == id {
                self.waiters.pop_front();
                return;
            }
        }
        // Fallback: Scan (O(N)). Only happens on cancellation.
        if let Some(pos) = self.waiters.iter().position(|w| w.id == id) {
            self.waiters.remove(pos);
        }
    }
}

impl FlowController {
    pub fn new(max_credits: usize) -> Self {
        Self {
            inner: Rc::new(RefCell::new(Inner {
                available: max_credits,
                max_credits,
                next_waiter_id: 0,
                waiters: VecDeque::with_capacity(32),
            })),
        }
    }

    pub fn acquire(&self) -> Acquire {
        Acquire::new(self.inner.clone(), 1)
    }

    pub fn acquire_many(&self, count: u32) -> Acquire {
        Acquire::new(self.inner.clone(), count as usize)
    }

    /// Acquire a single credit and return a guard that releases it on drop.
    pub async fn acquire_guard(&self) -> FlowControlGuard {
        self.acquire_guard_many(1).await
    }

    pub async fn acquire_guard_many(&self, count: u32) -> FlowControlGuard {
        let size = count as usize;
        self.acquire_many(count).await;
        FlowControlGuard { 
            inner: self.inner.clone(), 
            size 
        }
    }

    #[allow(dead_code)]
    pub fn try_acquire(&self, count: u32) -> bool {
        let needed = count as usize;
        let mut inner = self.inner.borrow_mut();
        
        if !inner.waiters.is_empty() {
            return false;
        }

        if inner.available >= needed {
            inner.available -= needed;
            true
        } else {
            false
        }
    }

    /// Get the number of currently available credits.
    /// Used for smart signal decisions in the send path.
    #[inline]
    pub fn available(&self) -> usize {
        self.inner.borrow().available
    }

    pub fn release(&self, count: usize) {
        if count == 0 { return; }

        let waker = {
            let mut inner = self.inner.borrow_mut();
            inner.available = inner.available.saturating_add(count).min(inner.max_credits);
            inner.ready_head_waker()
        };
        
        if let Some(w) = waker {
            w.wake();
        }
    }
}

pub struct Acquire {
    inner: Rc<RefCell<Inner>>,
    needed: usize,
    waiter_id: Option<u64>,
}

impl Acquire {
    fn new(inner: Rc<RefCell<Inner>>, needed: usize) -> Self {
        let max = inner.borrow().max_credits;
        assert!(needed <= max, "Deadlock: acquire({}) > max({})", needed, max);
        Self { inner, needed, waiter_id: None }
    }
}

impl Future for Acquire {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Safety: Prevent double-polling after Ready
        if self.needed == 0 {
            return Poll::Ready(());
        }

        let mut wake_next: Option<Waker> = None;
        let mut ready = false;
        let mut new_waiter_id: Option<u64> = None;

        {
            let mut inner = self.inner.borrow_mut();

            // Logic Flow:
            // 1. If we have a waiter_id, we are in the queue.
            // 2. If we are in the queue, we can only acquire if we are Head AND credits available.
            // 3. If we are NOT in the queue (first poll), we can acquire if Queue Empty AND credits available.
            
            // Optimization: determine if we are effectively at the head
            let is_head = match self.waiter_id {
                Some(id) => inner.waiters.front().map(|w| w.id == id).unwrap_or(false),
                None => inner.waiters.is_empty(),
            };

            if is_head && inner.available >= self.needed {
                // --- Success Path ---
                inner.available -= self.needed;

                if self.waiter_id.is_some() {
                    // We verified we are head above, so pop_front is correct
                    inner.waiters.pop_front();
                }

                // Chain reaction: check if the *next* waiter can also run
                wake_next = inner.ready_head_waker();
                
                // Clean state (after we're done with inner)
                ready = true;
            } else {
                // --- Wait Path ---
                match self.waiter_id {
                    Some(id) => {
                        // We are already queued. Update Waker.
                        // NOTE: This O(N) scan is unfortunate but necessary with VecDeque 
                        // to handle waker updates (e.g. task moving).
                        if let Some(pos) = inner.waiters.iter().position(|w| w.id == id) {
                            inner.waiters[pos].waker = cx.waker().clone();
                        } else {
                            // This branch implies state corruption or logic bug (ID set but not in queue).
                            // Recover by re-queueing to avoid hanging forever.
                            let id = inner.next_waiter_id;
                            inner.next_waiter_id = inner.next_waiter_id.wrapping_add(1);
                            let needed = self.needed;
                            inner.waiters.push_back(Waiter {
                                id,
                                needed,
                                waker: cx.waker().clone(),
                            });
                            // Defer waiter_id update until after borrow ends
                            new_waiter_id = Some(id);
                        }
                    }
                    None => {
                        // Initial queue insertion
                        let id = inner.next_waiter_id;
                        inner.next_waiter_id = inner.next_waiter_id.wrapping_add(1);
                        let needed = self.needed;
                        inner.waiters.push_back(Waiter {
                            id,
                            needed,
                            waker: cx.waker().clone(),
                        });
                        // Defer waiter_id update until after borrow ends
                        new_waiter_id = Some(id);
                    }
                }
            }
        } // End borrow

        // Apply deferred waiter_id update now that inner is dropped
        if let Some(id) = new_waiter_id {
            self.as_mut().get_mut().waiter_id = Some(id);
        }

        if let Some(w) = wake_next {
            w.wake();
        }

        if ready {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Acquire {
    fn drop(&mut self) {
        // If we acquired (needed == 0) or were never queued (waiter_id == None), nothing to do.
        if self.needed == 0 {
            return;
        }
        if let Some(id) = self.waiter_id {
            let waker = {
                let mut inner = self.inner.borrow_mut();
                inner.remove_waiter(id);
                // If the head was removed (us), the new head might be runnable
                inner.ready_head_waker()
            };
            if let Some(w) = waker {
                w.wake();
            }
        }
    }
}

pub struct FlowControlGuard {
    inner: Rc<RefCell<Inner>>,
    size: usize,
}

impl Drop for FlowControlGuard {
    fn drop(&mut self) {
        if self.size == 0 { return; }
        
        // This is safe because Rc<RefCell<Inner>> ensures the Inner is still alive.
        // If FlowController was dropped, inner is still held by this Guard.
        let waker = {
            // Unlikely to fail borrow unless recursive logic exists (not present here)
            if let Ok(mut inner) = self.inner.try_borrow_mut() {
                inner.available = inner.available.saturating_add(self.size).min(inner.max_credits);
                inner.ready_head_waker()
            } else {
                // In case of a panic unwinding while borrow is held elsewhere,
                // we leak credits rather than double-panicking.
                None
            }
        };

        if let Some(w) = waker {
            w.wake();
        }
    }
}