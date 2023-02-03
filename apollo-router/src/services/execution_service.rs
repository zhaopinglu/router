//! Implements the Execution phase of the request lifecycle.

use std::future::ready;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::channel::mpsc;
use futures::channel::mpsc::Receiver;
use futures::channel::mpsc::SendError;
use futures::channel::mpsc::Sender;
use futures::future::BoxFuture;
use futures::stream::once;
use futures::FutureExt;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use serde_json_bytes::Value;
use tower::BoxError;
use tower::ServiceBuilder;
use tower::ServiceExt;
use tower_service::Service;
use tracing::Instrument;
use uuid::Uuid;

use super::layers::allow_only_http_post_mutations::AllowOnlyHttpPostMutationsLayer;
use super::new_service::ServiceFactory;
use super::Plugins;
use super::SubgraphServiceFactory;
use crate::graphql;
use crate::graphql::IncrementalResponse;
use crate::graphql::Response;
use crate::json_ext::Object;
use crate::json_ext::Path;
use crate::json_ext::PathElement;
use crate::json_ext::ValueExt;
use crate::notification::Notify;
use crate::query_planner::subscription::SubscriptionHandle;
use crate::services::execution;
use crate::services::ExecutionRequest;
use crate::services::ExecutionResponse;
use crate::spec::Schema;

pub(crate) const SUBSCRIPTION_ID_KEY_CONTEXT: &str = "query_plan::subscription_id";

/// [`Service`] for query execution.
#[derive(Clone)]
pub(crate) struct ExecutionService {
    pub(crate) schema: Arc<Schema>,
    pub(crate) subgraph_service_factory: Arc<SubgraphServiceFactory>,
    pub(crate) notify: Notify<Uuid, graphql::Response>,
}

// Used to detect when the stream is dropped and then when the client closed the connection
pub(crate) struct StreamWrapper(pub(crate) Receiver<Response>, Option<SubscriptionHandle>);

impl Stream for StreamWrapper {
    type Item = Response;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

impl Drop for StreamWrapper {
    fn drop(&mut self) {
        if let Some(mut subscription_handle) = self.1.take() {
            subscription_handle
                .notify
                .try_delete(subscription_handle.id);
        }
    }
}

impl Service<ExecutionRequest> for ExecutionService {
    type Response = ExecutionResponse;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        // We break backpressure here.
        // We can implement backpressure, but we need to think about what we want out of it.
        // For instance, should be block all services if one downstream service is not ready?
        // This may not make sense if you have hundreds of services.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ExecutionRequest) -> Self::Future {
        let clone = self.clone();

        let this = std::mem::replace(self, clone);

        let fut = async move {
            let context = req.context;
            let ctx = context.clone();
            let (sender, receiver) = mpsc::channel(10);
            let variables = req.supergraph_request.body().variables.clone();
            let operation_name = req.supergraph_request.body().operation_name.clone();

            let is_deferred = req
                .query_plan
                .is_deferred(operation_name.as_deref(), &variables);
            let is_subscription = req
                .query_plan
                .is_subscription(operation_name.as_deref());
            let subscription_handle = if is_subscription {
                let subscription_id = Uuid::new_v4();
                let _ = ctx.insert(SUBSCRIPTION_ID_KEY_CONTEXT, subscription_id).expect("cannot put subscription id in the context");
                Some(SubscriptionHandle::new(subscription_id, this.notify.clone()))
            } else {
                None
            };

            let mut first = req
                .query_plan
                .execute(
                    &context,
                    &this.subgraph_service_factory,
                    &Arc::new(req.supergraph_request),
                    &this.schema,
                    sender,
                    subscription_handle.clone(),
                )
                .await;
            let query = req.query_plan.query.clone();
            let stream = if is_deferred || is_subscription {
                let stream_mode = if is_deferred {
                    StreamMode::Defer
                } else {
                    first.subscribed = Some(true);
                    StreamMode::Subscription
                };
                let stream = filter_stream(first, receiver, stream_mode);
                StreamWrapper(stream, subscription_handle).boxed()
            } else {
                once(ready(first)).chain(receiver).boxed()
            };

            let schema = this.schema.clone();
            let mut nullified_paths: Vec<Path> = vec![];

            let stream = stream
                .filter_map(move |mut response: Response| {
                    // responses that would fall under a path that was previously nullified are not sent
                    if nullified_paths.iter().any(|path| match &response.path {
                        None => false,
                        Some(response_path) => response_path.starts_with(path),
                    }) {
                        if response.has_next == Some(false) {
                            return ready(Some(Response::builder().has_next(false).build()));
                        } else {
                            return ready(None);
                        }
                    }

                    let has_next = response.has_next.unwrap_or(true);
                    tracing::debug_span!("format_response").in_scope(|| {
                        let paths = query.format_response(
                            &mut response,
                            operation_name.as_deref(),
                            is_deferred,
                            variables.clone(),
                            schema.api_schema(),
                        );

                        nullified_paths.extend(paths.into_iter());
                    });

                    match (response.path.as_ref(), response.data.as_ref()) {
                        // For subscription we don't provide a path
                        (None, Some(_)) => {
                            ready(Some(response))
                        }
                        (None, _) | (_, None) => {
                            if is_deferred {
                                response.has_next = Some(has_next);
                            }

                            response.errors.retain(|error| match &error.path {
                                    None => true,
                                    Some(error_path) => query.contains_error_path(operation_name.as_deref(), response.subselection.as_deref(), response.path.as_ref(), error_path),
                                });
                            ready(Some(response))
                        }
                        // if the deferred response specified a path, we must extract the
                        // values matched by that path and create a separate response for
                        // each of them.
                        // While { "data": { "a": { "b": 1 } } } and { "data": { "b": 1 }, "path: ["a"] }
                        // would merge in the same ways, some clients will generate code
                        // that checks the specific type of the deferred response at that
                        // path, instead of starting from the root object, so to support
                        // this, we extract the value at that path.
                        // In particular, that means that a deferred fragment in an object
                        // under an array would generate one response par array element
                        (Some(response_path), Some(response_data)) => {
                            let mut sub_responses = Vec::new();
                            response_data.select_values_and_paths(response_path, |path, value| {
                                // if the deferred path points to an array, split it into multiple subresponses
                                // because the root must be an object
                                if let Value::Array(array) = value {
                                    let mut parent = path.clone();
                                    for (i, value) in array.iter().enumerate() {
                                        parent.push(PathElement::Index(i));
                                        sub_responses.push((parent.clone(), value.clone()));
                                        parent.pop();
                                    }
                                } else {
                                    sub_responses.push((path.clone(), value.clone()));
                                }
                            });

                            let query = query.clone();
                            let operation_name = operation_name.clone();

                            let incremental = sub_responses
                                .into_iter()
                                .filter_map(move |(path, data)| {
                                    // filter errors that match the path of this incremental response
                                    let errors = response
                                        .errors
                                        .iter()
                                        .filter(|error| match &error.path {
                                            None => false,
                                            Some(error_path) => query.contains_error_path(operation_name.as_deref(), response.subselection.as_deref(), response.path.as_ref(), error_path) &&  error_path.starts_with(&path),

                                        })
                                        .cloned()
                                        .collect::<Vec<_>>();

                                        let extensions: Object = response
                                        .extensions
                                        .iter()
                                        .map(|(key, value)| {
                                            if key.as_str() == "valueCompletion" {
                                                let value = match value.as_array() {
                                                    None => Value::Null,
                                                    Some(v) => Value::Array(
                                                        v.iter()
                                                            .filter(|ext| {
                                                                match ext
                                                                    .as_object()
                                                                    .as_ref()
                                                                    .and_then(|ext| {
                                                                        ext.get("path")
                                                                    })
                                                                    .and_then(|v| {
                                                                        let p:Option<Path> = serde_json_bytes::from_value(v.clone()).ok();
                                                                        p
                                                                    }) {
                                                                    None => false,
                                                                    Some(ext_path) => {
                                                                        ext_path
                                                                            .starts_with(
                                                                                &path,
                                                                            )
                                                                    }
                                                                }
                                                            })
                                                            .cloned()
                                                            .collect(),
                                                    ),
                                                };

                                                (key.clone(), value)
                                            } else {
                                                (key.clone(), value.clone())
                                            }
                                        })
                                        .collect();

                                    // an empty response should not be sent
                                    // still, if there's an error or extension to show, we should
                                    // send it
                                    if !data.is_null()
                                        || !errors.is_empty()
                                        || !extensions.is_empty()
                                    {
                                        Some(
                                            IncrementalResponse::builder()
                                                .and_label(response.label.clone())
                                                .data(data)
                                                .path(path)
                                                .errors(errors)
                                                .extensions(extensions)
                                                .build(),
                                        )
                                    } else {
                                        None
                                    }
                                })
                                .collect();

                            ready(Some(
                                Response::builder()
                                    .has_next(has_next)
                                    .incremental(incremental)
                                    .build(),
                            ))

                        }
                    }
                })
                .boxed();

            Ok(ExecutionResponse::new_from_response(
                http::Response::new(stream as _),
                ctx,
            ))
        }
        .in_current_span();
        Box::pin(fut)
    }
}

#[derive(Clone, Copy)]
enum StreamMode {
    Defer,
    Subscription,
}

// modifies the response stream to set `has_next` to `false` and `subscribed` to `false` on the last response
fn filter_stream(
    first: Response,
    mut stream: Receiver<Response>,
    stream_mode: StreamMode,
) -> Receiver<Response> {
    let (mut sender, receiver) = mpsc::channel(10);

    tokio::task::spawn(async move {
        let mut seen_last_message =
            consume_responses(first, &mut stream, &mut sender, stream_mode).await?;

        while let Some(current_response) = stream.next().await {
            seen_last_message =
                consume_responses(current_response, &mut stream, &mut sender, stream_mode).await?;
        }

        // the response stream disconnected early so we could not add `has_next = false` to the
        // last message, so we add an empty one
        if !seen_last_message {
            let res = match stream_mode {
                StreamMode::Defer => Response::builder().has_next(false).build(),
                StreamMode::Subscription => Response::builder().subscribed(false).build(),
            };
            sender.send(res).await?;
        }

        Ok::<_, SendError>(())
    });

    receiver
}

// returns Ok(true) when we saw the last message
async fn consume_responses(
    mut current_response: Response,
    stream: &mut Receiver<Response>,
    sender: &mut Sender<Response>,
    stream_mode: StreamMode,
) -> Result<bool, SendError> {
    loop {
        match stream.try_next() {
            // no messages available, but the channel is not closed
            // this means more deferred responses can come
            Err(_) => {
                sender.send(current_response).await?;
                return Ok(false);
            }

            // there might be other deferred responses after this one,
            // so we should call `try_next` again
            Ok(Some(response)) => {
                sender.send(current_response).await?;
                current_response = response;
            }
            // the channel is closed
            // there will be no other deferred responses after that,
            // so we set `has_next` to `false`
            Ok(None) => {
                match stream_mode {
                    StreamMode::Defer => current_response.has_next = Some(false),
                    StreamMode::Subscription => current_response.subscribed = Some(false),
                }

                sender.send(current_response).await?;
                return Ok(true);
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct ExecutionServiceFactory {
    pub(crate) schema: Arc<Schema>,
    pub(crate) plugins: Arc<Plugins>,
    pub(crate) subgraph_service_factory: Arc<SubgraphServiceFactory>,
    pub(crate) notify: Notify<Uuid, graphql::Response>,
}

impl ServiceFactory<ExecutionRequest> for ExecutionServiceFactory {
    type Service = execution::BoxService;

    fn create(&self) -> Self::Service {
        ServiceBuilder::new()
            .layer(AllowOnlyHttpPostMutationsLayer::default())
            .service(
                self.plugins.iter().rev().fold(
                    crate::services::execution_service::ExecutionService {
                        schema: self.schema.clone(),
                        subgraph_service_factory: self.subgraph_service_factory.clone(),
                        notify: self.notify.clone(),
                    }
                    .boxed(),
                    |acc, (_, e)| e.execution_service(acc),
                ),
            )
            .boxed()
    }
}
