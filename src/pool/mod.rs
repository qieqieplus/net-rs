use crate::transport::Transport;
use rustc_hash::FxHashMap;
use std::hash::Hash;
use std::rc::Rc;
use std::cell::RefCell;
use std::time::{Duration, Instant};

/// A pooled connection wrapper that tracks creation time for expiry.
pub struct PooledConnection<T: Transport> {
    inner: Rc<T>,
    created_at: Instant,
}

impl<T: Transport> PooledConnection<T> {
    pub fn new(transport: T) -> Self {
        Self {
            inner: Rc::new(transport),
            created_at: Instant::now(),
        }
    }

    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }

    pub fn transport(&self) -> &Rc<T> {
        &self.inner
    }
}

/// A connection pool that manages transport connections.
/// 
/// Generic over:
/// - K: Key type (e.g., Address)
/// - T: Transport type
/// 
/// Note: This pool is Thread-Per-Core (single threaded). 
/// It uses Rc/RefCell and is !Send / !Sync.
pub struct ConnectionPool<K, T>
where
    K: Eq + Hash + Clone,
    T: Transport,
{
    pool: Rc<RefCell<FxHashMap<K, PooledConnection<T>>>>,
    ttl: Duration,
}

impl<K, T> ConnectionPool<K, T>
where
    K: Eq + Hash + Clone + 'static,
    T: Transport,
{
    pub fn new(ttl: Duration) -> Self {
        Self {
            pool: Rc::new(RefCell::new(FxHashMap::default())),
            ttl,
        }
    }

    /// Get a connection from the pool, or None if not cached or expired.
    pub async fn get(&self, key: &K) -> Option<Rc<T>> {
        let pool = self.pool.borrow();
        pool.get(key).and_then(|conn| {
            if conn.is_expired(self.ttl) {
                None
            } else {
                Some(conn.transport().clone())
            }
        })
    }

    /// Insert a new connection into the pool.
    pub async fn insert(&self, key: K, transport: T) -> Rc<T> {
        let conn = PooledConnection::new(transport);
        let rc = conn.transport().clone();
        let mut pool = self.pool.borrow_mut();
        pool.insert(key, conn);
        rc
    }

    /// Remove a specific connection from the pool.
    pub async fn remove(&self, key: &K) {
        let mut pool = self.pool.borrow_mut();
        pool.remove(key);
    }

    /// Clean up expired connections.
    pub async fn cleanup_expired(&self) {
        let mut pool = self.pool.borrow_mut();
        pool.retain(|_, conn| !conn.is_expired(self.ttl));
    }

    /// Get the number of active connections.
    pub async fn len(&self) -> usize {
        let pool = self.pool.borrow();
        pool.len()
    }

    /// Check if the pool is empty.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}


pub mod manager;
