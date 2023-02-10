use futures::channel::mpsc;
use futures::future;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use serde_json_bytes::Value;
use tower::ServiceExt;
use tracing_futures::Instrument;
use uuid::Uuid;

use super::execution::ExecutionParameters;
use super::fetch::Variables;
use super::OperationKind;
use crate::error::FetchError;
use crate::graphql;
use crate::graphql::Error;
use crate::graphql::Request;
use crate::graphql::Response;
use crate::http_ext;
use crate::json_ext::Object;
use crate::json_ext::Path;
use crate::notification::Notify;
use crate::plugins::subscription::CallbackMode;
use crate::plugins::subscription::SubscriptionMode;
use crate::plugins::subscription::SUBSCRIPTION_MODE_CONTEXT_KEY;
use crate::query_planner::SUBSCRIBE_SPAN_NAME;
use crate::services::SubgraphRequest;

#[derive(Clone)]
pub(crate) struct SubscriptionHandle {
    pub(crate) id: Uuid,
    pub(crate) notify: Notify<Uuid, graphql::Response>,
}

impl SubscriptionHandle {
    pub(crate) fn new(id: Uuid, notify: Notify<Uuid, graphql::Response>) -> Self {
        Self { id, notify }
    }
}

pub(crate) struct SubscriptionNode {
    /// The name of the service or subgraph that the fetch is querying.
    pub(crate) service_name: String,

    /// The variables that are used for the subgraph fetch.
    pub(crate) variable_usages: Vec<String>,

    /// The GraphQL subquery that is used for the fetch.
    pub(crate) operation: String,

    /// The GraphQL subquery operation name.
    pub(crate) operation_name: Option<String>,
}

impl SubscriptionNode {
    pub(crate) fn execute_recursively<'a>(
        &'a self,
        parameters: &'a ExecutionParameters<'a>,
        current_dir: &'a Path,
        parent_value: &'a Value,
        mut sender: futures::channel::mpsc::Sender<Response>,
        mut subscription_handle: SubscriptionHandle,
    ) -> future::BoxFuture<Vec<Error>> {
        let mode: SubscriptionMode = match parameters
            .context
            .get(SUBSCRIPTION_MODE_CONTEXT_KEY)
            .ok()
            .flatten()
        {
            Some(mode) => mode,
            None => {
                return Box::pin(async {
                    vec![Error::builder()
                        .message("subscription support is not enabled")
                        .extension_code("SUBSCRIPTION_DISABLED")
                        .build()]
                });
            }
        };

        Box::pin(async move {
            let mut cloned_qp = parameters.root_node.clone();
            // FIXME: Trick, should not exist with the correct qp implementation
            cloned_qp.without_subscription();

            let current_dir_cloned = current_dir.clone();
            let context = parameters.context.clone();
            let service_factory = parameters.service_factory.clone();
            let schema = parameters.schema.clone();
            let supergraph_request = parameters.supergraph_request.clone();
            let deferred_fetches = parameters.deferred_fetches.clone();
            let query = parameters.query.clone();
            let subscription_id = subscription_handle.id;

            tracing::trace!("Generated subscription ID: {subscription_id}");

            match mode {
                SubscriptionMode::Passthrough(_) => {
                    let (tx_gql, mut rx_gql) = mpsc::channel::<
                        Box<dyn Stream<Item = graphql::Response> + Send + Unpin>,
                    >(1);

                    let _subscription_task = tokio::task::spawn(async move {
                        let cloned_qp = cloned_qp;
                        let parameters = ExecutionParameters {
                            context: &context,
                            service_factory: &service_factory,
                            schema: &schema,
                            supergraph_request: &supergraph_request,
                            deferred_fetches: &deferred_fetches,
                            query: &query,
                            root_node: &cloned_qp,
                        };

                        let gql_ws_stream = match rx_gql.next().await {
                            Some(ws) => ws,
                            None => {
                                tracing::error!("cannot get the graphql websocket stream");
                                return;
                            }
                        };

                        Self::task(
                            gql_ws_stream,
                            &parameters,
                            &current_dir_cloned,
                            sender.clone(),
                            subscription_handle.id,
                        )
                        .await;
                    });

                    let fetch_time_offset =
                        parameters.context.created_at.elapsed().as_nanos() as i64;
                    match self
                        .websocket_call(parameters, current_dir, parent_value, tx_gql)
                        .instrument(tracing::info_span!(
                            SUBSCRIBE_SPAN_NAME,
                            "otel.kind" = "INTERNAL",
                            "apollo.subgraph.name" = self.service_name.as_str(),
                            "apollo_private.sent_time_offset" = fetch_time_offset
                        ))
                        .await
                    {
                        Ok(e) => e,
                        Err(err) => {
                            failfast_error!("websocket call fetch error: {}", err);
                            vec![err.to_graphql_error(Some(current_dir.to_owned()))]
                        }
                    }
                }
                SubscriptionMode::Callback(CallbackMode { public_url, .. }) => {
                    let _subscription_task = tokio::task::spawn(async move {
                        let mut handle = match subscription_handle
                            .notify
                            .subscribe(subscription_handle.id)
                            .await
                        {
                            Ok(handle) => handle,
                            Err(err) => {
                                tracing::error!(
                                    "cannot subscribe for subscription id {}: {err:?}",
                                    subscription_handle.id
                                );
                                let _ = sender
                                .send(
                                    Response::builder()
                                        .errors(vec![
                                            Error::builder().message(format!("cannot subscribe for subscription id {}: {err:?}", subscription_handle.id)).extension_code("SUBSCRIPTION_ID_NOT_FOUND").build()
                                        ])
                                        .build(),
                                )
                                .await;
                                return;
                            }
                        };
                        // When it will be part of a function the receiver will be different between websocket or pubsub
                        let receiver = handle.receiver();

                        // ======================================================
                        let cloned_qp = cloned_qp;
                        let parameters = ExecutionParameters {
                            context: &context,
                            service_factory: &service_factory,
                            schema: &schema,
                            supergraph_request: &supergraph_request,
                            deferred_fetches: &deferred_fetches,
                            query: &query,
                            root_node: &cloned_qp,
                        };

                        Self::task(
                            receiver,
                            &parameters,
                            &current_dir_cloned,
                            sender.clone(),
                            subscription_handle.id,
                        )
                        .await;
                    });

                    let callback_url = match public_url
                        .join(&format!("/callback/{subscription_id}"))
                    {
                        Ok(callback_url) => callback_url,
                        Err(err) => {
                            return vec![Error::builder()
                                .message(format!("subscription public url is incorrect: {err:?}"))
                                .extension_code("INVALID_SUBSCRIPTION_PUBLIC_URL")
                                .build()];
                        }
                    };

                    let fetch_time_offset =
                        parameters.context.created_at.elapsed().as_nanos() as i64;
                    match self
                        .subscribe_callback(parameters, current_dir, parent_value, callback_url)
                        .instrument(tracing::info_span!(
                            SUBSCRIBE_SPAN_NAME,
                            "otel.kind" = "INTERNAL",
                            "apollo.subgraph.name" = self.service_name.as_str(),
                            "apollo_private.sent_time_offset" = fetch_time_offset
                        ))
                        .await
                    {
                        Ok(e) => e,
                        Err(err) => {
                            failfast_error!("Subscription callback fetch error: {}", err);
                            vec![err.to_graphql_error(Some(current_dir.to_owned()))]
                        }
                    }
                }
            }
        })
    }

    pub(crate) async fn task<'a>(
        mut receiver: impl Stream<Item = graphql::Response> + Unpin,
        parameters: &'a ExecutionParameters<'a>,
        current_dir: &'a Path,
        mut sender: futures::channel::mpsc::Sender<Response>,
        subscription_id: Uuid,
    ) {
        let mut cloned_qp = parameters.root_node.clone();
        cloned_qp.without_subscription();

        while let Some(mut val) = receiver.next().await {
            let (value, subselection, mut errors) = cloned_qp
                .execute_recursively(
                    parameters,
                    current_dir,
                    &val.data.unwrap_or_default(),
                    sender.clone(),
                    None,
                )
                .await;
            errors.append(&mut val.errors);

            if let Err(err) = sender
                .send(
                    Response::builder()
                        .data(value)
                        .subscribed(true)
                        .and_subselection(subselection)
                        .errors(errors)
                        .extensions(val.extensions)
                        .and_path(val.path)
                        .build(),
                )
                .await
            {
                tracing::error!("cannot send the subscription to the client: {err:?}");
                break;
            }
        }
        tracing::trace!("Leaving the task for subscription {}", subscription_id);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn subscribe_callback<'a>(
        &'a self,
        parameters: &'a ExecutionParameters<'a>,
        current_dir: &'a Path,
        data: &Value,
        callback_url: url::Url,
    ) -> Result<Vec<Error>, FetchError> {
        let SubscriptionNode {
            operation,
            operation_name,
            service_name,
            ..
        } = self;

        let Variables { variables, .. } = match Variables::new(
            &[],
            self.variable_usages.as_ref(),
            data,
            current_dir,
            // Needs the original request here
            parameters.supergraph_request,
            parameters.schema,
            &None, // TODO: check if it's something we should do also for subscriptions
        )
        .await
        {
            Some(variables) => variables,
            None => {
                return Ok(Vec::new()); // I'm doing the same than in fetch node but not sure about this
            }
        };
        let mut extensions = Object::new();
        extensions.insert(
            "callback_url",
            Value::String(callback_url.to_string().into()),
        );
        let subgraph_request = SubgraphRequest::builder()
            .supergraph_request(parameters.supergraph_request.clone())
            .subgraph_request(
                http_ext::Request::builder()
                    .method(http::Method::POST)
                    .uri(
                        parameters
                            .schema
                            .subgraph_url(service_name)
                            .unwrap_or_else(|| {
                                panic!(
                                    "schema uri for subgraph '{service_name}' should already have been checked"
                                )
                            })
                            .clone(),
                    )
                    .body(
                        Request::builder()
                            .query(operation)
                            .and_operation_name(operation_name.clone())
                            .variables(variables.clone())
                            .extensions(extensions)
                            .build(),
                    )
                    .build()
                    .expect("it won't fail because the url is correct and already checked; qed"),
            )
            .operation_kind(OperationKind::Subscription)
            .context(parameters.context.clone())
            .build();

        let service = parameters
            .service_factory
            .create(service_name)
            .expect("we already checked that the service exists during planning; qed");

        let (_parts, response) = service
            .oneshot(subgraph_request)
            .instrument(tracing::trace_span!("subscription_callback_stream"))
            .await
            // TODO this is a problem since it restores details about failed service
            // when errors have been redacted in the include_subgraph_errors module.
            // Unfortunately, not easy to fix here, because at this point we don't
            // know if we should be redacting errors for this subgraph...
            .map_err(|e| FetchError::SubrequestHttpError {
                service: service_name.to_string(),
                reason: e.to_string(),
            })?
            .response
            .into_parts();

        Ok(response.errors)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn websocket_call<'a>(
        &'a self,
        parameters: &'a ExecutionParameters<'a>,
        current_dir: &'a Path,
        data: &Value,
        tx_gql: mpsc::Sender<Box<dyn Stream<Item = graphql::Response> + Send + Unpin>>,
    ) -> Result<Vec<Error>, FetchError> {
        let SubscriptionNode {
            operation,
            operation_name,
            service_name,
            ..
        } = self;

        let Variables { variables, .. } = match Variables::new(
            &[],
            self.variable_usages.as_ref(),
            data,
            current_dir,
            // Needs the original request here
            parameters.supergraph_request,
            parameters.schema,
            &None, // TODO: check if it's something we should do also for subscriptions
        )
        .await
        {
            Some(variables) => variables,
            None => {
                return Ok(Vec::new()); // FIXME ?
            }
        };

        let subgraph_request = SubgraphRequest::builder()
            .supergraph_request(parameters.supergraph_request.clone())
            .subgraph_request(
                http_ext::Request::builder()
                    .method(http::Method::POST)
                    .uri(
                        parameters
                            .schema
                            .subgraph_url(service_name)
                            .unwrap_or_else(|| {
                                panic!(
                                    "schema uri for subgraph '{service_name}' should already have been checked"
                                )
                            })
                            .clone(),
                    )
                    .body(
                        Request::builder()
                            .query(operation)
                            .and_operation_name(operation_name.clone())
                            .variables(variables.clone())
                            .build(),
                    )
                    .build()
                    .expect("it won't fail because the url is correct and already checked; qed"),
            )
            .operation_kind(OperationKind::Subscription)
            .context(parameters.context.clone())
            .ws_stream(tx_gql)
            .build();

        let service = parameters
            .service_factory
            .create(service_name)
            .expect("we already checked that the service exists during planning; qed");

        let (_parts, response) = service
            .oneshot(subgraph_request)
            .instrument(tracing::trace_span!("subscription_callback_stream"))
            .await
            // TODO this is a problem since it restores details about failed service
            // when errors have been redacted in the include_subgraph_errors module.
            // Unfortunately, not easy to fix here, because at this point we don't
            // know if we should be redacting errors for this subgraph...
            .map_err(|e| FetchError::SubrequestWsError {
                service: service_name.to_string(),
                reason: e.to_string(),
            })?
            .response
            .into_parts();

        Ok(response.errors)
    }
}
