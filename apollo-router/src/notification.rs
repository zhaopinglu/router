//! Internal pub/sub facility for subscription

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

use futures::channel::mpsc;
use futures::channel::mpsc::SendError;
use futures::channel::oneshot;
use futures::SinkExt;
use futures::StreamExt;
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
        tokio::task::spawn(task(receiver));
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

async fn task<K, V>(mut receiver: mpsc::Receiver<Notification<K, V>>)
where
    K: Send + Hash + Eq + Clone + 'static,
    V: Send + Clone + 'static,
{
    let mut pubsub: PubSub<K, V> = PubSub::default();

    while let Some(message) = receiver.next().await {
        match message {
            Notification::Subscribe {
                topic,
                handle,
                sender,
            } => pubsub.subscribe(topic, handle, sender).await,
            Notification::Unsubscribe { handle } => pubsub.unsubscribe(handle).await,
            Notification::Delete { topic } => pubsub.delete(topic).await,
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
                    pubsub.subscribe(topic, handle, sender).await;
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
    }
}

struct PubSub<K, V>
where
    K: Hash + Eq,
{
    subscribers: HashMap<Uuid, mpsc::Sender<V>>,
    subscriptions: HashMap<K, HashSet<Uuid>>,
}

impl<K, V> Default for PubSub<K, V>
where
    K: Hash + Eq,
{
    fn default() -> Self {
        Self {
            subscribers: HashMap::new(),
            subscriptions: HashMap::new(),
        }
    }
}

impl<K, V> PubSub<K, V>
where
    K: Hash + Eq + Clone,
{
    async fn subscribe(&mut self, topic: K, handle: Uuid, sender: mpsc::Sender<V>) {
        self.subscribers.insert(handle, sender);
        self.subscriptions
            .entry(topic)
            .and_modify(|e| {
                e.insert(handle);
            })
            .or_insert_with(|| [handle].into());
    }

    async fn unsubscribe(&mut self, handle: Uuid) {
        self.subscribers.remove(&handle);
        let mut topics_to_delete = vec![];
        for (topic, handles) in &mut self.subscriptions {
            handles.remove(&handle);
            if handles.is_empty() {
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
                !s.difference(&[handle].into())
                    .collect::<HashSet<&Uuid>>()
                    .is_empty()
            })
            .unwrap_or_default()
    }

    /// Check if the topic exists
    fn exist(&self, topic: &K) -> bool {
        self.subscriptions.contains_key(topic)
    }

    async fn delete(&mut self, topic: K) {
        if let Some(subscribers) = self.subscriptions.remove(&topic) {
            subscribers.into_iter().for_each(|s| {
                self.subscribers.remove(&s);
            });
        }
    }

    async fn publish(&mut self, topic: K, value: V) -> Option<()>
    where
        V: Clone,
    {
        let subscribers = self.subscriptions.get(&topic)?;
        let mut fut = vec![];
        for subscriber_handle in subscribers {
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
                .for_each(|(_topic, handles)| handles.retain(|h| h != &handle_to_clean));
        }
        self.subscriptions.retain(|_k, s| !s.is_empty());

        Some(())
    }

    #[cfg(test)]
    async fn broadcast(&mut self, value: V) -> Option<()>
    where
        V: Clone,
    {
        let mut fut = vec![];
        for subscribers in self.subscriptions.values() {
            for subscriber_handle in subscribers {
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
                .for_each(|(_topic, handles)| handles.retain(|h| h != &handle_to_clean));
        }
        self.subscriptions.retain(|_k, s| !s.is_empty());

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
    }
}
