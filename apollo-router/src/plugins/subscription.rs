use std::collections::HashMap;
use std::ops::ControlFlow;
use std::str::FromStr;
use std::task::Context;
use std::task::Poll;

use bytes::Buf;
use futures::future::BoxFuture;
use http::Method;
use http::StatusCode;
use multimap::MultiMap;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use tower::BoxError;
use tower::Service;
use tower::ServiceBuilder;
use tower::ServiceExt;
use uuid::Uuid;

use crate::graphql;
use crate::graphql::Response;
use crate::json_ext::Object;
use crate::layers::ServiceBuilderExt;
use crate::notification::Notify;
use crate::plugin::Plugin;
use crate::plugin::PluginInit;
use crate::protocols::websocket::WebSocketProtocol;
use crate::query_planner::OperationKind;
use crate::register_plugin;
use crate::services::router;
use crate::services::subgraph;
use crate::services::supergraph;
use crate::Endpoint;
use crate::ListenAddr;

pub(crate) const SUBSCRIPTION_MODE_CONTEXT_KEY: &str = "subscription::mode";

#[derive(Debug, Clone)]
struct Subscription {
    enabled: bool,
    notify: Notify<Uuid, graphql::Response>,
    mode: SubscriptionMode,
}

/// Forbid mutations configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, default)]
struct SubscriptionConfig {
    /// Enable subscription support
    enabled: bool,
    /// Select a subscription mode (callback or passthrough)
    mode: SubscriptionMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SubscriptionMode {
    /// Using a callback url
    #[serde(rename = "callback")]
    Callback(CallbackMode),
    /// Using websocket to directly connect to subgraph
    #[serde(rename = "passthrough")]
    Passthrough(PassthroughMode),
}

/// Using a callback url
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
pub(crate) struct CallbackMode {
    #[schemars(with = "String")]
    /// URL used to access this router instance
    pub(crate) public_url: url::Url,
    // `skip_serializing` We don't need it in the context
    /// Listen address on which the callback must listen (default: 127.0.0.1:4000)
    #[serde(skip_serializing)]
    listen: Option<ListenAddr>,
    // `skip_serializing` We don't need it in the context
    /// Specify on which path you want to listen for callbacks (default: /callback)
    #[serde(skip_serializing)]
    path: Option<String>,
}

/// Using websocket to directly connect to subgraph
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(default)]
pub(crate) struct PassthroughMode {
    /// URL used to access this router instance
    pub(crate) subgraphs: HashMap<String, WebSocketConfiguration>,
}

impl Default for SubscriptionMode {
    fn default() -> Self {
        // TODO change this default ?
        Self::Passthrough(PassthroughMode::default())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(default)]
pub(crate) struct WebSocketConfiguration {
    pub(crate) path: Option<String>,
    pub(crate) protocol: WebSocketProtocol,
}

fn default_path() -> String {
    String::from("/callback")
}

fn default_listen_addr() -> ListenAddr {
    ListenAddr::SocketAddr("127.0.0.1:4000".parse().expect("valid ListenAddr"))
}

#[async_trait::async_trait]
impl Plugin for Subscription {
    type Config = SubscriptionConfig;

    async fn new(init: PluginInit<Self::Config>) -> Result<Self, BoxError> {
        Ok(Subscription {
            enabled: true,
            notify: init.notify,
            mode: init.config.mode,
        })
    }

    fn supergraph_service(&self, service: supergraph::BoxService) -> supergraph::BoxService {
        let mode = self.mode.clone();
        ServiceBuilder::new()
            .map_request(move |req: supergraph::Request| {
                req.context
                    .insert(SUBSCRIPTION_MODE_CONTEXT_KEY, mode.clone())
                    .unwrap();
                req
            })
            .service(service)
            .boxed()
    }

    fn subgraph_service(
        &self,
        _subgraph_name: &str,
        service: subgraph::BoxService,
    ) -> subgraph::BoxService {
        let enabled = self.enabled;
        ServiceBuilder::new()
            .checkpoint(move |req: subgraph::Request| {
                if req.operation_kind == OperationKind::Subscription && !enabled {
                    Ok(ControlFlow::Break(subgraph::Response::builder().context(req.context).error(graphql::Error::builder().message("cannot execute a subscription if it's not enabled in the configuration").extension_code("SUBSCRIPTION_DISABLED").build()).extensions(Object::default()).build()))
                } else {
                    Ok(ControlFlow::Continue(req))
                }
            }).service(service)
            .boxed()
    }

    fn web_endpoints(&self) -> MultiMap<ListenAddr, Endpoint> {
        let mut map = MultiMap::new();

        if let SubscriptionMode::Callback(CallbackMode { listen, path, .. }) = &self.mode {
            let path = path.clone().unwrap_or_else(default_path);
            if self.enabled {
                let endpoint = Endpoint::from_router_service(
                    format!("{}/:callback", path.trim_end_matches('/')),
                    CallbackService::new(self.notify.clone()).boxed(),
                );
                map.insert(listen.clone().unwrap_or_else(default_listen_addr), endpoint);
            }
        }

        map
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "kind", rename = "lowercase")]
pub(crate) enum CallbackPayload {
    #[serde(rename = "subscription")]
    Subscription { data: Response },
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename = "lowercase")]
pub(crate) enum CallbackKind {
    #[serde(rename = "subscription")]
    Subscription,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct DeleteCallbackPayload {
    kind: CallbackKind,
}

#[derive(Clone)]
pub(crate) struct CallbackService {
    notify: Notify<Uuid, graphql::Response>,
}

impl CallbackService {
    pub(crate) fn new(notify: Notify<Uuid, graphql::Response>) -> Self {
        Self { notify }
    }
}

impl Service<router::Request> for CallbackService {
    type Response = router::Response;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, req: router::Request) -> Self::Future {
        let mut notify = self.notify.clone();
        Box::pin(async move {
            let (parts, body) = req.router_request.into_parts();
            let sub_id =
                match uuid::Uuid::from_str(parts.uri.path().trim_start_matches("/callback/")) {
                    Ok(sub_id) => sub_id,
                    Err(_) => {
                        return Ok(router::Response {
                            response: http::Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body::<hyper::Body>("cannot convert the subscription id".into())
                                .map_err(BoxError::from)?,
                            context: req.context,
                        });
                    }
                };

            match parts.method {
                Method::POST => {
                    let cb_body = hyper::body::to_bytes(body)
                        .await
                        .map_err(|e| format!("failed to get the request body: {e}"))
                        .and_then(|bytes| {
                            serde_json::from_reader::<_, CallbackPayload>(bytes.reader()).map_err(
                                |err| {
                                    format!(
                                        "failed to deserialize the request body into JSON: {err}"
                                    )
                                },
                            )
                        });
                    let cb_body = match cb_body {
                        Ok(cb_body) => cb_body,
                        Err(err) => {
                            return Ok(router::Response {
                                response: http::Response::builder()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(err.into())
                                    .map_err(BoxError::from)?,
                                context: req.context,
                            });
                        }
                    };

                    match cb_body {
                        CallbackPayload::Subscription { data } => {
                            let mut handle = match notify.subscribe_if_exist(sub_id).await {
                                Some(handle) => handle,
                                None => {
                                    return Ok(router::Response {
                                        response: http::Response::builder()
                                            .status(StatusCode::NOT_FOUND)
                                            .body("suscription doesn't exist".into())
                                            .map_err(BoxError::from)?,
                                        context: req.context,
                                    });
                                }
                            };

                            handle.publish(sub_id, data).await?;
                        }
                    }

                    Ok(router::Response {
                        response: http::Response::builder()
                            .status(StatusCode::OK)
                            .body::<hyper::Body>("".into())
                            .map_err(BoxError::from)?,
                        context: req.context,
                    })
                }
                Method::DELETE => {
                    let cb_body = hyper::body::to_bytes(body)
                        .await
                        .map_err(|e| format!("failed to get the request body: {e}"))
                        .and_then(|bytes| {
                            serde_json::from_reader::<_, DeleteCallbackPayload>(bytes.reader())
                                .map_err(|err| {
                                    format!(
                                        "failed to deserialize the request body into JSON: {err}"
                                    )
                                })
                        });
                    let cb_body = match cb_body {
                        Ok(cb_body) => cb_body,
                        Err(err) => {
                            return Ok(router::Response {
                                response: http::Response::builder()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(err.into())
                                    .map_err(BoxError::from)?,
                                context: req.context,
                            });
                        }
                    };

                    match cb_body.kind {
                        CallbackKind::Subscription => {
                            notify.try_delete(sub_id);
                            Ok(router::Response {
                                response: http::Response::builder()
                                    .status(StatusCode::ACCEPTED)
                                    .body::<hyper::Body>("".into())
                                    .map_err(BoxError::from)?,
                                context: req.context,
                            })
                        }
                    }
                }
                _ => Ok(router::Response {
                    response: http::Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body::<hyper::Body>("".into())
                        .map_err(BoxError::from)?,
                    context: req.context,
                }),
            }
        })
    }
}

// #[cfg(test)]
// mod tests {
//     use http::Method;
//     use http::StatusCode;
//     use serde_json::json;
//     use tower::ServiceExt;

//     use super::*;
//     use crate::graphql;
//     use crate::graphql::Response;
//     use crate::http_ext::Request;
//     use crate::plugin::test::MockExecutionService;
//     use crate::plugin::PluginInit;
//     use crate::query_planner::fetch::OperationKind;
//     use crate::query_planner::PlanNode;
//     use crate::query_planner::QueryPlan;

//     #[tokio::test]
//     async fn it_lets_queries_pass_through() {
//         let mut mock_service = MockExecutionService::new();

//         mock_service
//             .expect_call()
//             .times(1)
//             .returning(move |_| Ok(ExecutionResponse::fake_builder().build().unwrap()));

//         let service_stack = ForbidMutations::new(PluginInit::new(
//             ForbidMutationsConfig(true),
//             Default::default(),
//         ))
//         .await
//         .expect("couldn't create forbid_mutations plugin")
//         .execution_service(mock_service.boxed());

//         let request = create_request(Method::GET, OperationKind::Query);

//         let _ = service_stack
//             .oneshot(request)
//             .await
//             .unwrap()
//             .next_response()
//             .await
//             .unwrap();
//     }

//     #[tokio::test]
//     async fn it_doesnt_let_mutations_pass_through() {
//         let expected_error = Error::builder()
//             .message("Mutations are forbidden".to_string())
//             .extension_code("MUTATION_FORBIDDEN")
//             .build();
//         let expected_status = StatusCode::BAD_REQUEST;

//         let service_stack = ForbidMutations::new(PluginInit::new(
//             ForbidMutationsConfig(true),
//             Default::default(),
//         ))
//         .await
//         .expect("couldn't create forbid_mutations plugin")
//         .execution_service(MockExecutionService::new().boxed());
//         let request = create_request(Method::GET, OperationKind::Mutation);

//         let mut actual_error = service_stack.oneshot(request).await.unwrap();

//         assert_eq!(expected_status, actual_error.response.status());
//         assert_error_matches(&expected_error, actual_error.next_response().await.unwrap());
//     }

//     #[tokio::test]
//     async fn configuration_set_to_false_lets_mutations_pass_through() {
//         let mut mock_service = MockExecutionService::new();

//         mock_service
//             .expect_call()
//             .times(1)
//             .returning(move |_| Ok(ExecutionResponse::fake_builder().build().unwrap()));

//         let service_stack = ForbidMutations::new(PluginInit::new(
//             ForbidMutationsConfig(false),
//             Default::default(),
//         ))
//         .await
//         .expect("couldn't create forbid_mutations plugin")
//         .execution_service(mock_service.boxed());

//         let request = create_request(Method::GET, OperationKind::Mutation);

//         let _ = service_stack
//             .oneshot(request)
//             .await
//             .unwrap()
//             .next_response()
//             .await
//             .unwrap();
//     }

//     fn assert_error_matches(expected_error: &Error, response: Response) {
//         assert_eq!(&response.errors[0], expected_error);
//     }

//     fn create_request(method: Method, operation_kind: OperationKind) -> ExecutionRequest {
//         let root: PlanNode = if operation_kind == OperationKind::Mutation {
//             serde_json::from_value(json!({
//                 "kind": "Sequence",
//                 "nodes": [
//                     {
//                         "kind": "Fetch",
//                         "serviceName": "product",
//                         "variableUsages": [],
//                         "operation": "{__typename}",
//                         "operationKind": "mutation"
//                       },
//                 ]
//             }))
//             .unwrap()
//         } else {
//             serde_json::from_value(json!({
//                 "kind": "Sequence",
//                 "nodes": [
//                     {
//                         "kind": "Fetch",
//                         "serviceName": "product",
//                         "variableUsages": [],
//                         "operation": "{__typename}",
//                         "operationKind": "query"
//                       },
//                 ]
//             }))
//             .unwrap()
//         };

//         let request = Request::fake_builder()
//             .method(method)
//             .body(graphql::Request::default())
//             .build()
//             .expect("expecting valid request");
//         ExecutionRequest::fake_builder()
//             .supergraph_request(request)
//             .query_plan(QueryPlan::fake_builder().root(root).build())
//             .build()
//     }
// }

register_plugin!("apollo", "subscription", Subscription);
