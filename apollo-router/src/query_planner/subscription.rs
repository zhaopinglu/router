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
use crate::http_ext;
use crate::json_ext::Object;
use crate::json_ext::Path;
use crate::notification::Notify;
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

    /// Optional id used by Deferred nodes
    pub(crate) id: Option<String>,
}

impl SubscriptionNode {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn subscribe_callback<'a>(
        &'a self,
        parameters: &'a ExecutionParameters<'a>,
        current_dir: &'a Path,
        data: &Value,
        callback_url: String,
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
        extensions.insert("callback_url", callback_url.into());
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
