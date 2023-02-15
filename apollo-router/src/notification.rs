//! Internal pub/sub facility for subscription

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;
use std::time::Instant;

use futures::channel::mpsc;
use futures::channel::mpsc::SendError;
use futures::channel::oneshot;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use tokio_stream::wrappers::IntervalStream;
use uuid::Uuid;

pub(crate) type NotifyError = SendError;

enum Notification<K, V> {
    Subscribe {
        topic: K,
        handle: Uuid,
        // Sender to send value we will receive
        sender: mpsc::Sender<V>,
    },
    Unsubscribe {
        handle: Uuid,
    },
    Delete {
        topic: K,
    },
    KeepAlive {
        topic: K,
    },
    Publish {
        topic: K,
        data: V,
    },
    Exist {
        topic: K,
        response_sender: oneshot::Sender<bool>,
    },
    SubscribeIfExist {
        topic: K,
        handle: Uuid,
        sender: mpsc::Sender<V>,
        response_sender: oneshot::Sender<bool>,
    },
    #[cfg(test)]
    Broadcast {
        data: V,
    },
}

/// In memory pub/sub implementation
#[derive(Clone)]
pub struct Notify<K, V> {
    sender: mpsc::Sender<Notification<K, V>>,
}

impl<K, V> Notify<K, V>
where
    K: Send + Hash + Eq + Clone + 'static,
    V: Send + Clone + 'static,
{
    pub(crate) fn new() -> Notify<K, V> {
        let (sender, receiver) = mpsc::channel(10000);
        tokio::task::spawn(task(receiver, None));
        Notify { sender }
    }

    /// If you want to create a notify with a TTL on unused subscription
    pub(crate) fn with_ttl(ttl: Duration) -> Notify<K, V> {
        let (sender, receiver) = mpsc::channel(10000);
        tokio::task::spawn(task(receiver, Some(ttl)));
        Notify { sender }
    }

    pub(crate) async fn subscribe(&mut self, topic: K) -> Result<Handle<K, V>, NotifyError> {
        let (sender, receiver) = mpsc::channel(10);
        let id = Uuid::new_v4();
        let handle = Handle {
            receiver,
            id,
            sender: self.sender.clone(),
        };

        self.sender
            .send(Notification::Subscribe {
                handle: handle.id,
                topic,
                sender,
            })
            .await?;

        Ok(handle)
    }

    pub(crate) async fn subscribe_if_exist(&mut self, topic: K) -> Option<Handle<K, V>> {
        let (sender, receiver) = mpsc::channel(10);
        let id = Uuid::new_v4();
        let handle = Handle {
            receiver,
            id,
            sender: self.sender.clone(),
        };
        // Channel to check if the topic still exists or not
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(Notification::SubscribeIfExist {
                handle: handle.id,
                topic,
                sender,
                response_sender: response_tx,
            })
            .await
            .ok()?;

        match response_rx.await {
            Ok(true) => Some(handle),
            _ => None,
        }
    }

    // TODO improve error handling here
    pub(crate) async fn exist(&mut self, topic: K) -> Result<bool, NotifyError> {
        // Channel to check if the topic still exists or not
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(Notification::Exist {
                topic,
                response_sender: response_tx,
            })
            .await?;

        match response_rx.await {
            Ok(true) => Ok(true),
            _ => Ok(false),
        }
    }

    pub(crate) async fn keep_alive(&mut self, topic: K) -> Result<(), NotifyError> {
        self.sender.send(Notification::KeepAlive { topic }).await?;

        Ok(())
    }

    pub(crate) fn try_delete(&mut self, topic: K) {
        // if disconnected, we don't care (the task was stopped)
        let _ = self.sender.try_send(Notification::Delete { topic });
    }

    #[allow(dead_code)]
    pub(crate) async fn publish(&mut self, topic: K, data: V) -> Result<(), NotifyError> {
        self.sender
            .send(Notification::Publish { topic, data })
            .await?;

        Ok(())
    }

    #[doc(hidden)]
    /// Useless notify mainly for test
    pub fn noop() -> Self {
        let (sender, _receiver) = mpsc::channel(2);
        Notify { sender }
    }

    // Only for tests
    #[cfg(test)]
    pub(crate) async fn broadcast(&mut self, data: V) -> Result<(), NotifyError> {
        self.sender.send(Notification::Broadcast { data }).await?;

        Ok(())
    }
}

#[cfg(test)]
impl<K, V> Default for Notify<K, V>
where
    K: Send + Hash + Eq + Clone + 'static,
    V: Send + Clone + 'static,
{
    /// Useless notify mainly for test
    fn default() -> Self {
        Self::noop()
    }
}

impl<K, V> Debug for Notify<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Notify").finish()
    }
}
pub(crate) struct Handle<K, V> {
    receiver: mpsc::Receiver<V>,
    id: Uuid,
    sender: mpsc::Sender<Notification<K, V>>,
}

impl<K, V> Handle<K, V> {
    #[allow(dead_code)]
    pub(crate) async fn unsubscribe(mut self) {
        // if disconnected, we don't care (the task was stopped)
        // if the channel is full, can we let it leak in the task?
        let _ = self
            .sender
            .send(Notification::Unsubscribe { handle: self.id })
            .await;
    }

    pub(crate) fn receiver(&mut self) -> &mut mpsc::Receiver<V> {
        &mut self.receiver
    }

    pub(crate) async fn publish(&mut self, topic: K, data: V) -> Result<(), NotifyError> {
        self.sender
            .send(Notification::Publish { topic, data })
            .await?;

        Ok(())
    }
}

impl<K, V> Drop for Handle<K, V> {
    fn drop(&mut self) {
        let _ = self
            .sender
            .try_send(Notification::Unsubscribe { handle: self.id });
    }
}

async fn task<K, V>(mut receiver: mpsc::Receiver<Notification<K, V>>, ttl: Option<Duration>)
where
    K: Send + Hash + Eq + Clone + 'static,
    V: Send + Clone + 'static,
{
    let mut pubsub: PubSub<K, V> = PubSub::new(ttl);

    let mut ttl_fut: Box<dyn Stream<Item = tokio::time::Instant> + Send + Unpin> = match ttl {
        Some(ttl) => Box::new(IntervalStream::new(tokio::time::interval(ttl))),
        None => Box::new(tokio_stream::pending()),
    };

    loop {
        tokio::select! {
            _ = ttl_fut.next() => {
                pubsub.kill_dead_topics();
                pubsub.clean();
            }
            message = receiver.next() => {
                if ttl.is_none() {
                    pubsub.clean();
                }
                match message {
                    Some(message) => {
                        match message {
                            Notification::Subscribe {
                                topic,
                                handle,
                                sender,
                            } => pubsub.subscribe(topic, handle, sender),
                            Notification::Unsubscribe { handle } => pubsub.unsubscribe(handle),
                            Notification::KeepAlive { topic } => pubsub.touch(&topic),
                            Notification::Delete { topic } => pubsub.delete(topic),
                            Notification::Publish { topic, data } => {
                                pubsub.publish(topic, data).await;
                            }
                            Notification::SubscribeIfExist {
                                topic,
                                handle,
                                sender,
                                response_sender,
                            } => {
                                if pubsub.is_used(&topic, handle) {
                                    let _ = response_sender.send(true);
                                    pubsub.subscribe(topic, handle, sender);
                                } else {
                                    let _ = response_sender.send(false);
                                }
                            }
                            Notification::Exist {
                                topic,
                                response_sender,
                            } => {
                                let _ = response_sender.send(pubsub.exist(&topic));
                            }
                            #[cfg(test)]
                            Notification::Broadcast { data } => {
                                pubsub.broadcast(data).await;
                            }
                        }
                    },
                    None => break,
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Subscription {
    subscribers: HashSet<Uuid>,
    updated_at: Instant,
}

impl Subscription {
    // Update the updated_at value
    fn touch(&mut self) {
        self.updated_at = Instant::now();
    }
}

impl<const N: usize> From<[Uuid; N]> for Subscription {
    fn from(value: [Uuid; N]) -> Self {
        Self {
            subscribers: value.into(),
            updated_at: Instant::now(),
        }
    }
}

struct PubSub<K, V>
where
    K: Hash + Eq,
{
    subscribers: HashMap<Uuid, mpsc::Sender<V>>,
    subscriptions: HashMap<K, Subscription>,
    ttl: Option<Duration>,
}

impl<K, V> Default for PubSub<K, V>
where
    K: Hash + Eq,
{
    fn default() -> Self {
        Self {
            subscribers: HashMap::new(),
            subscriptions: HashMap::new(),
            ttl: None,
        }
    }
}

impl<K, V> PubSub<K, V>
where
    K: Hash + Eq + Clone,
{
    fn new(ttl: Option<Duration>) -> Self {
        Self {
            subscribers: HashMap::new(),
            subscriptions: HashMap::new(),
            ttl,
        }
    }

    fn subscribe(&mut self, topic: K, handle: Uuid, sender: mpsc::Sender<V>) {
        self.subscribers.insert(handle, sender);
        self.subscriptions
            .entry(topic)
            .and_modify(|e| {
                e.touch();
                e.subscribers.insert(handle);
            })
            .or_insert_with(|| [handle].into());
    }

    fn unsubscribe(&mut self, handle: Uuid) {
        self.subscribers.remove(&handle);
        let mut topics_to_delete = vec![];
        for (topic, sub) in &mut self.subscriptions {
            sub.touch();
            sub.subscribers.remove(&handle);
            if sub.subscribers.is_empty() {
                topics_to_delete.push(topic.clone());
            }
        }
        topics_to_delete.iter().for_each(|t| {
            self.subscriptions.remove(t);
        });
    }

    /// Check if the topic is used by anyone else than the current handle
    fn is_used(&self, topic: &K, handle: Uuid) -> bool {
        self.subscriptions
            .get(topic)
            .map(|s| {
                !s.subscribers
                    .difference(&[handle].into())
                    .collect::<HashSet<&Uuid>>()
                    .is_empty()
            })
            .unwrap_or_default()
    }

    /// Check if the topic exists
    fn touch(&mut self, topic: &K) {
        if let Some(sub) = self.subscriptions.get_mut(topic) {
            sub.touch();
        }
    }

    /// Check if the topic exists
    fn exist(&self, topic: &K) -> bool {
        self.subscriptions.contains_key(topic)
    }

    // TODO this might be useless thanks to the drop impl of handle
    /// clean all closed channels
    fn clean(&mut self) {
        self.subscriptions.retain(|_topic, sub| {
            sub.subscribers.retain(|id| match self.subscribers.get(id) {
                Some(s) => {
                    if s.is_closed() {
                        self.subscribers.remove(id);
                        false
                    } else {
                        true
                    }
                }
                None => false,
            });
            !sub.subscribers.is_empty()
        });
        // Delete subscribers linked to 0 subscription
        self.subscribers.retain(|id, _| {
            self.subscriptions
                .values()
                .flat_map(|sub| &sub.subscribers)
                .any(|e| e == id)
        })
    }

    /// clean all topics which didn't heartbeat
    fn kill_dead_topics(&mut self) {
        if let Some(ttl) = self.ttl {
            self.subscriptions
                .retain(|_topic, sub| sub.updated_at.elapsed() <= ttl);
        }
    }

    fn delete(&mut self, topic: K) {
        if let Some(sub) = self.subscriptions.remove(&topic) {
            sub.subscribers.into_iter().for_each(|s| {
                self.subscribers.remove(&s);
            });
        }
    }

    async fn publish(&mut self, topic: K, value: V) -> Option<()>
    where
        V: Clone,
    {
        let subscription = self.subscriptions.get_mut(&topic)?;
        subscription.touch();

        let mut fut = vec![];
        for subscriber_handle in &subscription.subscribers {
            if let Some(mut sender) = self.subscribers.get(subscriber_handle).cloned() {
                let cloned_value = value.clone();
                fut.push(async move {
                    sender
                        .send(cloned_value)
                        .await
                        .is_err()
                        .then_some(*subscriber_handle)
                });
            }
        }
        // clean closed sender
        let handles_to_clean = futures::future::join_all(fut).await.into_iter().flatten();
        for handle_to_clean in handles_to_clean {
            self.subscribers.remove(&handle_to_clean);
            self.subscriptions
                .iter_mut()
                .for_each(|(_topic, sub)| sub.subscribers.retain(|h| h != &handle_to_clean));
        }
        self.subscriptions.retain(|_k, s| !s.subscribers.is_empty());

        Some(())
    }

    #[cfg(test)]
    async fn broadcast(&mut self, value: V) -> Option<()>
    where
        V: Clone,
    {
        let mut fut = vec![];
        for sub in self.subscriptions.values() {
            for subscriber_handle in &sub.subscribers {
                if let Some(mut sender) = self.subscribers.get(subscriber_handle).cloned() {
                    let cloned_value = value.clone();
                    fut.push(async move {
                        sender
                            .send(cloned_value)
                            .await
                            .is_err()
                            .then_some(*subscriber_handle)
                    });
                }
            }
        }
        // clean closed sender
        let handles_to_clean = futures::future::join_all(fut).await.into_iter().flatten();
        for handle_to_clean in handles_to_clean {
            self.subscribers.remove(&handle_to_clean);
            self.subscriptions
                .iter_mut()
                .for_each(|(_topic, sub)| sub.subscribers.retain(|h| h != &handle_to_clean));
        }
        self.subscriptions.retain(|_k, s| !s.subscribers.is_empty());

        Some(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn subscribe() {
        let mut notify = Notify::new();
        let topic_1 = Uuid::new_v4();
        let topic_2 = Uuid::new_v4();

        let handle1 = notify.subscribe(topic_1).await.unwrap();
        let _handle2 = notify.subscribe(topic_2).await.unwrap();

        let mut handle_1_bis = notify.subscribe(topic_1).await.unwrap();
        let mut handle_1_other = notify.subscribe(topic_1).await.unwrap();
        let mut cloned_notify = notify.clone();
        tokio::spawn(async move {
            cloned_notify
                .publish(topic_1, serde_json_bytes::json!({"test": "ok"}))
                .await
                .unwrap();
        });
        drop(handle1);

        let new_msg = handle_1_bis.receiver().next().await.unwrap();
        assert_eq!(new_msg, serde_json_bytes::json!({"test": "ok"}));
        let new_msg = handle_1_other.receiver().next().await.unwrap();
        assert_eq!(new_msg, serde_json_bytes::json!({"test": "ok"}));

        assert!(notify.exist(topic_1).await.unwrap());
        assert!(notify.exist(topic_2).await.unwrap());
    }

    #[tokio::test]
    async fn it_test_ttl() {
        let mut notify = Notify::with_ttl(Duration::from_millis(100));
        let topic_1 = Uuid::new_v4();
        let topic_2 = Uuid::new_v4();

        let handle1 = notify.subscribe(topic_1).await.unwrap();
        let _handle2 = notify.subscribe(topic_2).await.unwrap();

        let mut handle_1_bis = notify.subscribe(topic_1).await.unwrap();
        let mut handle_1_other = notify.subscribe(topic_1).await.unwrap();
        let mut cloned_notify = notify.clone();
        tokio::spawn(async move {
            cloned_notify
                .publish(topic_1, serde_json_bytes::json!({"test": "ok"}))
                .await
                .unwrap();
        });
        drop(handle1);

        let new_msg = handle_1_bis.receiver().next().await.unwrap();
        assert_eq!(new_msg, serde_json_bytes::json!({"test": "ok"}));
        let new_msg = handle_1_other.receiver().next().await.unwrap();
        assert_eq!(new_msg, serde_json_bytes::json!({"test": "ok"}));

        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(!notify.exist(topic_1).await.unwrap());
        assert!(!notify.exist(topic_2).await.unwrap());
    }
}
