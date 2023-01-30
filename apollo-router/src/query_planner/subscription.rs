use uuid::Uuid;

use crate::notification::Notify;

#[derive(Clone)]
pub(crate) struct SubscriptionHandle {
    pub(crate) id: Uuid,
    pub(crate) notify: Notify,

}

impl SubscriptionHandle {
    pub(crate) fn new(id: Uuid, notify: Notify) -> Self { Self { id, notify } }
}