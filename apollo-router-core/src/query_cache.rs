use futures::task::AtomicWaker;
use futures::Future;
use lru::LruCache;
use tokio::sync::Mutex;

use crate::prelude::graphql::*;
use crate::CacheCallback;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;

#[cfg(test)]
mod my_cache_tests {
    use crate::MyCacheBuilder;

    #[tokio::test]
    async fn it_works() {
        let is_above_20 = |n: &u8| n > &20;
        let my_cache = MyCacheBuilder::default()
            .with_size(10)
            .with_filler(Box::new(is_above_20))
            .build()
            .unwrap();

        assert_eq!(false, my_cache.get(&12).await.unwrap());
        assert_eq!(true, my_cache.get(&21).await.unwrap());

        // This is definitely not something we will do in real life,
        // but this helps a lot when testing:
        // Cache hits will stay the same, new calls will evolve
        let is_below_20 = |n: &u8| n < &20;
        let my_new_cache = MyCacheBuilder::default()
            .with_size(20)
            .with_filler(Box::new(is_below_20))
            .with_entries(my_cache.drain().await)
            .build()
            .unwrap();

        // cache hits (populated by is_above_20)
        assert_eq!(false, my_new_cache.get(&12).await.unwrap());
        assert_eq!(true, my_new_cache.get(&21).await.unwrap());

        // is below 20 hits
        assert_eq!(true, my_new_cache.get(&13).await.unwrap());
        assert_eq!(false, my_new_cache.get(&22).await.unwrap());
    }
}

// Mycache has a getter that takes a key, that only gets cloned if the cache is missed.
// The only way to get the underlying cache is to call drain, which shuts it down.
// Shutdown completes the current requests processing, but doesn't allow further cache population.
pub struct MyCache<'inflight, Key, Value>
where
    Key: Hash + Eq + Clone,
    Value: Clone,
{
    filler: Box<dyn Fn(&'inflight Key) -> Value>,
    cache: InnerCache<Key, Value>,
    in_flight: InFlight<'inflight, Key>,
}

impl<'inflight, Key, Value> MyCache<'inflight, Key, Value>
where
    Key: Hash + Eq + Clone,
    Value: Clone,
{
    pub async fn get(&self, key: &'inflight Key) -> Result<Value, CacheError> {
        if let Some(v) = self.cache.lock().await.get(key) {
            return Ok(v.clone());
        }

        self.in_flight.insert(key).await?;

        let computed_value = (*self.filler)(key);

        self.cache
            .lock()
            .await
            .put(key.clone(), computed_value.clone());

        self.in_flight.remove(key).await?;

        Ok(computed_value)
    }

    pub async fn drain(self) -> InnerCache<Key, Value> {
        let _ = self.in_flight.shutdown().await;
        self.cache
    }
}

// This design assumes each consumer of the datastructure has the same way of populating things.
// This may or may not actually be true
#[derive(Default)]
pub struct MyCacheBuilder<'inflight, Key, Value>
where
    Key: Hash + Eq + Clone,
    Value: Clone,
{
    size: Option<usize>,
    entries: Option<InnerCache<Key, Value>>,
    filler: Option<Box<dyn Fn(&'inflight Key) -> Value>>,
}

impl<'inflight, Key, Value> MyCacheBuilder<'inflight, Key, Value>
where
    Key: Hash + Eq + Clone,
    Value: Clone,
{
    pub fn with_size(self, size: usize) -> Self {
        Self {
            size: Some(size),
            ..self
        }
    }

    pub fn with_filler(self, filler: Box<dyn Fn(&'inflight Key) -> Value>) -> Self
    where
        Key: Hash + Eq + Clone,
        Value: Clone,
    {
        Self {
            filler: Some(filler),
            ..self
        }
    }

    pub fn with_entries(self, entries: InnerCache<Key, Value>) -> Self {
        Self {
            entries: Some(entries),
            ..self
        }
    }

    pub fn build(self) -> Result<MyCache<'inflight, Key, Value>, CacheError>
    where
        Key: Hash + Eq + Clone,
        Value: Clone,
    {
        if self.filler.is_none() || self.size.is_none() {
            Err(CacheError::CannotCreate)
        } else {
            let size = self.size.expect("checked above;qed");
            let cache = self
                .entries
                .unwrap_or_else(|| Arc::new(Mutex::new(LruCache::new(size))));
            Ok(MyCache {
                filler: self.filler.unwrap(),
                cache,
                in_flight: Default::default(),
            })
        }
    }
}

#[derive(Debug)]
pub enum CacheError {
    CannotCreate,
    Shuttingdown,
}

type InnerCache<Key, Value> = Arc<Mutex<LruCache<Key, Value>>>;

// In flight entry processing + wait until it's empty before shutting down
struct InFlight<'inflight, Key>
where
    Key: Hash + Eq + Clone,
{
    in_flight: Arc<Mutex<HashSet<&'inflight Key>>>,
    is_live: AtomicBool,
    shutdown_watcher: ShutdownWatcher,
}

impl<'inflight, Key> Default for InFlight<'inflight, Key>
where
    Key: Hash + Eq + Clone,
{
    fn default() -> Self {
        Self {
            in_flight: Default::default(),
            is_live: AtomicBool::new(true),
            shutdown_watcher: Default::default(),
        }
    }
}

impl<'inflight, Key> InFlight<'inflight, Key>
where
    Key: Hash + Eq + Clone,
{
    pub async fn insert(&self, key: &'inflight Key) -> Result<(), CacheError> {
        if !self.is_live.load(Ordering::SeqCst) {
            return Err(CacheError::Shuttingdown);
        }

        self.shutdown_watcher.increment();
        self.in_flight.lock().await.insert(key);

        Ok(())
    }

    pub async fn remove(&self, key: &'inflight Key) -> Result<(), CacheError> {
        self.in_flight.lock().await.remove(key);
        self.shutdown_watcher.decrement();
        Ok(())
    }

    pub async fn shutdown(self) -> Result<(), CacheError> {
        if self.is_live.swap(false, Ordering::SeqCst) {
            return Err(CacheError::Shuttingdown);
        }

        Ok(self.shutdown_watcher.await)
    }
}

// Shutdown watcher isn't ready until remaining_in_flights == 0
// It is polled when shutting down the Inflight
#[derive(Default)]
struct ShutdownWatcher {
    remaining_in_flights: AtomicUsize,
    waker: AtomicWaker,
}

impl ShutdownWatcher {
    pub fn increment(&self) {
        self.remaining_in_flights.fetch_add(1, Ordering::SeqCst);
    }

    pub fn decrement(&self) {
        self.remaining_in_flights.fetch_sub(1, Ordering::SeqCst);
        self.waker.wake()
    }
}

impl Future for ShutdownWatcher {
    type Output = ();
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.remaining_in_flights.load(Ordering::SeqCst) == 0 {
            return Poll::Ready(());
        }

        self.waker.register(cx.waker());

        if self.remaining_in_flights.load(Ordering::SeqCst) == 0 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

// --------------------------------------------------------------------------

/// A cache for parsed GraphQL queries.
#[derive(Debug)]
pub struct QueryCache {
    cm: CachingMap<QueryPlannerError, String, Option<Arc<Query>>>,
    schema: Arc<Schema>,
}

#[async_trait::async_trait]
impl CacheCallback<QueryPlannerError, String, Option<Arc<Query>>> for QueryCache {
    async fn delegated_get(&self, key: String) -> Result<Option<Arc<Query>>, QueryPlannerError> {
        let query_parsing_future = {
            let schema = Arc::clone(&self.schema);
            tokio::task::spawn_blocking(move || Query::parse(key, &schema))
        };
        let parsed_query = match query_parsing_future.await {
            Ok(res) => res.map(Arc::new),
            // Silently ignore cancelled tasks (never happen for blocking tasks).
            Err(err) if err.is_cancelled() => None,
            Err(err) => {
                failfast_debug!("Parsing query task failed: {}", err);
                None
            }
        };
        Ok(parsed_query)
    }
}

impl QueryCache {
    /// Instantiate a new cache for parsed GraphQL queries.
    pub fn new(cache_limit: usize, schema: Arc<Schema>) -> Self {
        let cm = CachingMap::new(cache_limit);
        Self { cm, schema }
    }

    /// Attempt to parse a string to a [`Query`] using cache if possible.
    pub async fn get_query(&self, query: impl AsRef<str>) -> Option<Arc<Query>> {
        let key = query.as_ref().to_string();
        /*
        let q = |key: String| async move {
            let query_parsing_future = {
                let schema = Arc::clone(&self.schema);
                tokio::task::spawn_blocking(move || Query::parse(key, &schema))
            };
            let parsed_query = match query_parsing_future.await {
                Ok(res) => res.map(Arc::new),
                // Silently ignore cancelled tasks (never happen for blocking tasks).
                Err(err) if err.is_cancelled() => None,
                Err(err) => {
                    failfast_debug!("Parsing query task failed: {}", err);
                    None
                }
            };
            Ok(parsed_query)
        };
        */

        match self.cm.get(self, key).await {
            Ok(v) => v,
            Err(err) => {
                failfast_debug!("Parsing query task failed: {}", err);
                None
            }
        }
    }
}
