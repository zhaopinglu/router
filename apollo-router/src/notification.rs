//! Internal pub/sub facility for invalidation
//!
//! The rules:
//! - any part of the router can subscribe to changes on a list of keys
//! - you get one notification at the first key that changes, no more after
//!  that (because a notification will trigger a new query, which means resubscribing on a different set of keys)
//! - you can cancel a subscription (drop the handle)
//! - you can send an invalidation notification for a list of keys
//! - subscriptions survive configuration or schema updates
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::AsyncRead;
use futures::Future;
use futures::SinkExt;
use futures::StreamExt;
use pin_project_lite::pin_project;
use rand::Rng;
use serde_json_bytes::Value;
use uuid::Uuid;

use crate::services::execution::QueryPlan;

enum Notification {
    Subscribe {
        // TODO use uuid
        topic: Uuid,
        handle: Uuid,
        // Sender to send value we will receive
        sender: mpsc::Sender<Value>,
    },
    Unsubscribe {
        handle: Uuid,
    },
    Delete {
        topic: Uuid,
    },
    Publish {
        topic: Uuid,
        data: Value,
    },
}

#[derive(Clone)]
pub(crate) struct Notify {
    sender: mpsc::Sender<Notification>,
}

impl Notify {
    pub(crate) fn new() -> Notify {
        Self::default()
    }

    pub(crate) async fn subscribe(&mut self, topic: Uuid) -> Handle {
        let (sender, receiver) = mpsc::channel(10);
        let id = Uuid::new_v4();
        let handle = Handle {
            receiver,
            id,
            sender: self.sender.clone(),
        };

        // FIXME: handle errors
        self.sender
            .send(Notification::Subscribe {
                handle: handle.id,
                topic,
                sender,
            })
            .await;

        handle
    }

    pub(crate) fn try_delete(&mut self, topic: Uuid) {
        // if disconnected, we don't care (the task was stopped)
        let _ = self.sender.try_send(Notification::Delete { topic });
    }

    pub(crate) async fn publish(&mut self, topic: Uuid, data: Value) {
        // FIXME: handle errors
        self.sender
            .send(Notification::Publish { topic, data })
            .await;
    }
}

impl Default for Notify {
    fn default() -> Self {
        let (sender, receiver) = mpsc::channel(10000);
        tokio::task::spawn(task(receiver));
        Notify { sender }
    }
}

impl Debug for Notify {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Notify").finish()
    }
}
pub(crate) struct Handle {
    receiver: mpsc::Receiver<Value>,
    id: Uuid,
    sender: mpsc::Sender<Notification>,
}

impl Handle {
    pub(crate) async fn unsubscribe(mut self) {
        // if disconnected, we don't care (the task was stopped)
        // if the channel is full, can we let it leak in the task?
        let _ = self
            .sender
            .send(Notification::Unsubscribe { handle: self.id })
            .await;
    }

    pub(crate) fn receiver(&mut self) -> &mut mpsc::Receiver<Value> {
        &mut self.receiver
    }
}

async fn task(mut receiver: mpsc::Receiver<Notification>) {
    let mut pubsub = PubSub::default();

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
        }
    }
}

#[derive(Default)]
struct PubSub {
    subscribers: HashMap<Uuid, mpsc::Sender<Value>>,
    subscriptions: HashMap<Uuid, HashSet<Uuid>>,
}

impl PubSub {
    async fn subscribe(&mut self, topic: Uuid, handle: Uuid, sender: mpsc::Sender<Value>) {
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
                topics_to_delete.push(*topic);
            }
        }
        topics_to_delete.iter().for_each(|t| {
            self.subscriptions.remove(t);
        });
    }

    async fn recycle(&mut self) {
        let mut subs_to_delete = HashSet::new();
        self.subscribers.retain(|sub_id, sub_chan| {
            if sub_chan.is_closed() {
                subs_to_delete.insert(*sub_id);
                false
            } else {
                true
            }
        });

        self.subscriptions = self
            .subscriptions
            .drain()
            .into_iter()
            .filter_map(|(topic, mut subscribers)| {
                subscribers = subscribers
                    .symmetric_difference(&subs_to_delete)
                    .cloned()
                    .collect();
                (!subscribers.is_empty()).then_some((topic, subscribers))
            })
            .collect();
    }

    async fn delete(&mut self, topic: Uuid) {
        if let Some(subscribers) = self.subscriptions.remove(&topic) {
            subscribers.into_iter().for_each(|s| {
                self.subscribers.remove(&s);
            });
        }
    }

    async fn publish(&mut self, topic: Uuid, value: Value) -> Option<()> {
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
        dbg!(&self.subscriptions);

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

        let handle1 = notify.subscribe(topic_1).await;
        let handle2 = notify.subscribe(topic_2).await;

        let mut handle_1_bis = notify.subscribe(topic_1).await;
        let mut handle_1_other = notify.subscribe(topic_1).await;
        let mut cloned_notify = notify.clone();
        tokio::spawn(async move {
            cloned_notify
                .publish(topic_1, serde_json_bytes::json!({"test": "ok"}))
                .await;
        });
        drop(handle1);

        let new_msg = handle_1_bis.receiver().next().await.unwrap();
        assert_eq!(new_msg, serde_json_bytes::json!({"test": "ok"}));
        let new_msg = handle_1_other.receiver().next().await.unwrap();
        assert_eq!(new_msg, serde_json_bytes::json!({"test": "ok"}));
    }
}
