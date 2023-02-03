use futures::future;
use futures::SinkExt;
use futures::StreamExt;
use serde_json_bytes::Value;
use tower::ServiceExt;
use tracing_futures::Instrument;
use uuid::Uuid;

use super::execution::ExecutionParameters;
use super::fetch::Variables;
use super::OperationKind;
use crate::error::FetchError;
use crate::graphql::Error;
use crate::graphql::Request;
use crate::graphql::Response;
use crate::http_ext;
use crate::json_ext::Object;
use crate::json_ext::Path;
use crate::notification::Notify;
use crate::plugins::subscription::SubscriptionMode;
use crate::plugins::subscription::SUBSCRIPTION_MODE_CONTEXT_KEY;
use crate::query_planner::FETCH_SPAN_NAME;
use crate::services::SubgraphRequest;

#[derive(Clone)]
pub(crate) struct SubscriptionHandle {
    pub(crate) id: Uuid,
    pub(crate) notify: Notify,
}

impl SubscriptionHandle {
    pub(crate) fn new(id: Uuid, notify: Notify) -> Self {
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
                SubscriptionMode::Passthrough => todo!(),
                SubscriptionMode::Callback { public_url, .. } => {
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
                    let root_node = parameters.root_node.clone();
                    let subscription_id = subscription_handle.id;

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

                    tracing::trace!("Generated subscription ID: {}", subscription_handle.id);
                    let _ = tokio::task::spawn(async move {
                        let mut handle = subscription_handle
                            .notify
                            .subscribe(subscription_handle.id)
                            .await;
                        let receiver = handle.receiver();
                        let cloned_qp = cloned_qp;
                        let parameters = ExecutionParameters {
                            context: &context,
                            service_factory: &service_factory,
                            schema: &schema,
                            supergraph_request: &supergraph_request,
                            deferred_fetches: &deferred_fetches,
                            query: &query,
                            root_node: &root_node,
                        };

                        // Take the remaining query plan

                        while let Some(mut val) = receiver.next().await {
                            let (value, subselection, mut errors) = cloned_qp
                                .execute_recursively(
                                    &parameters,
                                    &current_dir_cloned,
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
                                tracing::error!(
                                    "cannot send the subscription to the client: {err:?}"
                                );
                                break;
                            }
                        }
                        tracing::trace!(
                            "Leaving the task for subscription {}",
                            subscription_handle.id
                        );
                    });

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
        )
        .await
        {
            Some(variables) => variables,
            None => {
                return Ok(Vec::new());
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
                                    "schema uri for subgraph '{}' should already have been checked",
                                    service_name
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
}
