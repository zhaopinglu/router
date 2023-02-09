use std::collections::HashMap;

use futures::future;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use http::HeaderValue;
use http::Uri;
use serde_json_bytes::Value;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
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
use crate::plugins::override_url::OVERRIDE_URL_CONFIG_CONTEXT_KEY;
use crate::plugins::subscription::CallbackMode;
use crate::plugins::subscription::PassthroughMode;
use crate::plugins::subscription::SubscriptionMode;
use crate::plugins::subscription::WebSocketConfiguration;
use crate::plugins::subscription::SUBSCRIPTION_MODE_CONTEXT_KEY;
use crate::protocols::websocket::convert_websocket_stream;
use crate::protocols::websocket::GraphqlWebSocket;
use crate::protocols::websocket::WebSocketProtocol;
use crate::query_planner::FETCH_SPAN_NAME;
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
            match mode {
                SubscriptionMode::Passthrough(PassthroughMode { subgraphs }) => {
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
                    let subgraph_url = context
                        .get(OVERRIDE_URL_CONFIG_CONTEXT_KEY)
                        .ok()
                        .flatten()
                        .and_then(|urls_config: HashMap<String, String>| {
                            urls_config
                                .get(&self.service_name)
                                .and_then(|u| url::Url::parse(u).ok())
                        })
                        .or_else(|| {
                            schema
                                .subgraph(&self.service_name)
                                .and_then(|sub_url| url::Url::parse(&sub_url.to_string()).ok())
                        })
                        .unwrap_or_else(|| {
                            panic!(
                                "schema uri for subgraph '{}' should already have been checked",
                                self.service_name
                            )
                        });
                    let request = match self
                        .get_websocket_url(subgraph_url, subgraphs.get(&self.service_name))
                    {
                        Ok(req) => req,
                        Err(err) => return vec![err],
                    };

                    // Try to create an empty Stream + Sink and then connect it the to websocket stream afterwards in the subgraph service
                    // Use merge_stream ? use something else ?
                    let (ws_stream, _resp) = connect_async(request).await.unwrap();

                    let sub_uuid = Uuid::new_v4();
                    let gql_stream = match GraphqlWebSocket::new(
                        convert_websocket_stream(ws_stream, sub_uuid),
                        sub_uuid,
                    )
                    .await
                    {
                        Ok(gql_stream) => gql_stream,
                        Err(err) => {
                            tracing::error!("cannot create a graphql websocket stream: {err:?}");
                            return vec![Error::builder()
                                .message("cannot create a graphql websocket stream")
                                .extension_code("WEBSOCKET_STREAM_ERROR")
                                .build()];
                        }
                    };

                    let (mut gql_sink, gql_read_stream) = gql_stream.split();

                    let _subscription_task = tokio::task::spawn(async move {
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
                            gql_read_stream,
                            &parameters,
                            &current_dir_cloned,
                            sender.clone(),
                            subscription_handle.id,
                        )
                        .await;
                    });
                    let Variables { variables, .. } = match Variables::new(
                        &[],
                        self.variable_usages.as_ref(),
                        parent_value,
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
                            return Vec::new();
                        }
                    };

                    // TODO try to put this into subgraph service to have override plugin url for free
                    // Use connect_async on tokio_tungestenite with a channel
                    match gql_sink
                        .send(
                            graphql::Request::builder()
                                .query(self.operation.clone())
                                .and_operation_name(self.operation_name.clone())
                                .variables(variables)
                                .build(),
                        )
                        .await
                    {
                        Ok(_) => vec![],
                        Err(err) => {
                            vec![Error::builder()
                                .message(format!(
                                    "cannot send the subscription through websocket: {err:?}"
                                ))
                                .extension_code("WEBSOCKET_ERROR")
                                .build()]
                        }
                    }
                }
                SubscriptionMode::Callback(CallbackMode { public_url, .. }) => {
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

                    tracing::trace!("Generated subscription ID: {}", subscription_handle.id);
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
                    // ========================================================

                    let fetch_time_offset =
                        parameters.context.created_at.elapsed().as_nanos() as i64;
                    match self
                        .subscribe_callback(parameters, current_dir, parent_value, callback_url)
                        .instrument(tracing::info_span!(
                            FETCH_SPAN_NAME,
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

        // Take the remaining query plan

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
            // TODO: Re-Execute the query plan after subscription to aggregate data
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
                return Ok(Vec::new()); // FIXME ?
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
                            .subgraphs()
                            .find_map(|(name, url)| (name == service_name).then_some(url))
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
                return Ok(Vec::new()); // FIXME ?
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
                            .subgraphs()
                            .find_map(|(name, url)| (name == service_name).then_some(url))
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

    fn get_websocket_url(
        &self,
        mut subgraph_url: url::Url,
        subgraph_ws_cfg: Option<&WebSocketConfiguration>,
    ) -> Result<tungstenite::handshake::client::Request, graphql::Error> {
        let new_scheme = match subgraph_url.scheme() {
            "http" => "ws",
            "https" => "wss",
            _ => "ws",
        };
        subgraph_url.set_scheme(new_scheme).map_err(|err| {
            tracing::error!("cannot set a scheme '{new_scheme}' on subgraph url: {err:?}");
            graphql::Error::builder()
                .message("cannot set a scheme on websocket url")
                .extension_code("BAD_WEBSOCKET_URL")
                .build()
        })?;

        if let Some(WebSocketConfiguration { path, protocol }) = subgraph_ws_cfg {
            let subgraph_url = match path {
                Some(path) => subgraph_url.join(path).map_err(|_| {
                    graphql::Error::builder()
                        .message("cannot parse subgraph url with the specific websocket path")
                        .extension_code("BAD_WEBSOCKET_URL")
                        .build()
                })?,
                None => subgraph_url,
            };
            let mut request = subgraph_url.into_client_request().map_err(|err| {
                tracing::error!("cannot create websocket client request: {err:?}");

                graphql::Error::builder()
                    .message("cannot create websocket client request")
                    .extension_code("BAD_WEBSOCKET_REQUEST")
                    .build()
            })?;
            request
                .headers_mut()
                .insert(http::header::SEC_WEBSOCKET_PROTOCOL, (*protocol).into());

            Ok(request)
        } else {
            let mut request = subgraph_url.into_client_request().map_err(|err| {
                tracing::error!("cannot create websocket client request: {err:?}");

                graphql::Error::builder()
                    .message("cannot create websocket client request")
                    .extension_code("BAD_WEBSOCKET_REQUEST")
                    .build()
            })?;
            request.headers_mut().insert(
                http::header::SEC_WEBSOCKET_PROTOCOL,
                WebSocketProtocol::default().into(),
            );

            Ok(request)
        }
    }
}
