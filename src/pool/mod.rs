use crate::transport::Transport;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// A pooled connection wrapper that tracks creation time for expiry.
pub struct PooledConnection<T: Transport> {
    inner: Arc<T>,
    created_at: Instant,
}

impl<T: Transport> PooledConnection<T> {
    pub fn new(transport: T) -> Self {
        Self {
            inner: Arc::new(transport),
            created_at: Instant::now(),
        }
    }

    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }

    pub fn transport(&self) -> &Arc<T> {
        &self.inner
    }
}

/// A connection pool that manages transport connections.
/// 
/// Generic over:
/// - K: Key type (e.g., Address)
/// - T: Transport type
pub struct ConnectionPool<K, T>
where
    K: Eq + Hash + Clone,
    T: Transport,
{
    pool: Arc<RwLock<HashMap<K, PooledConnection<T>>>>,
    ttl: Duration,
}

impl<K, T> ConnectionPool<K, T>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    T: Transport,
{
    pub fn new(ttl: Duration) -> Self {
        Self {
            pool: Arc::new(RwLock::new(HashMap::new())),
            ttl,
        }
    }

    /// Get a connection from the pool, or None if not cached or expired.
    pub async fn get(&self, key: &K) -> Option<Arc<T>> {
        let pool = self.pool.read().await;
        pool.get(key).and_then(|conn| {
            if conn.is_expired(self.ttl) {
                None
            } else {
                Some(conn.transport().clone())
            }
        })
    }

    /// Insert a new connection into the pool.
    pub async fn insert(&self, key: K, transport: T) -> Arc<T> {
        let conn = PooledConnection::new(transport);
        let arc = conn.transport().clone();
        let mut pool = self.pool.write().await;
        pool.insert(key, conn);
        arc
    }

    /// Remove a specific connection from the pool.
    pub async fn remove(&self, key: &K) {
        let mut pool = self.pool.write().await;
        pool.remove(key);
    }

    /// Clean up expired connections.
    pub async fn cleanup_expired(&self) {
        let mut pool = self.pool.write().await;
        pool.retain(|_, conn| !conn.is_expired(self.ttl));
    }

    /// Get the number of active connections.
    pub async fn len(&self) -> usize {
        let pool = self.pool.read().await;
        pool.len()
    }
}

pub mod manager;
