use std::collections::HashMap;
use std::ops::ControlFlow;
use std::str::FromStr;
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

use crate::context::Context;
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
use crate::services::execution;
use crate::services::router;
use crate::services::subgraph;
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
    /// WebSocket configuration for specific subgraphs
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
/// WebSocket configuration for a specific subgraph
pub(crate) struct WebSocketConfiguration {
    /// Path on which WebSockets are listening
    pub(crate) path: Option<String>,
    /// Which WebSocket GraphQL protocol to use for this subgraph possible values are: 'graphql_ws' | 'subscriptions_transport_ws' (default: graphql_ws)
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
            enabled: init.config.enabled,
            notify: init.notify,
            mode: init.config.mode,
        })
    }

    fn execution_service(&self, service: execution::BoxService) -> execution::BoxService {
        let mode = self.mode.clone();
        ServiceBuilder::new()
            .map_request(move |req: execution::Request| {
                let operation_kind = req
                    .query_plan
                    .query
                    .operation(req.supergraph_request.body().operation_name.as_deref())
                    .map(|op| *op.kind())
                    .or_else(|| req.query_plan.query.operations.get(0).map(|op| *op.kind()));
                if let Some(OperationKind::Subscription) = operation_kind {
                    req.context
                        .insert(SUBSCRIPTION_MODE_CONTEXT_KEY, mode.clone())
                        .unwrap();
                }
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
                let path = path.trim_end_matches('/');
                let endpoint = Endpoint::from_router_service(
                    format!("{path}/:callback"),
                    CallbackService::new(self.notify.clone(), path.to_string()).boxed(),
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
    Subscription(SubscriptionPayload),
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "action", rename = "lowercase")]
pub(crate) enum SubscriptionPayload {
    #[serde(rename = "init")]
    Init { id: Uuid },
    #[serde(rename = "next")]
    Next { payload: Response, id: Uuid },
    #[serde(rename = "keep_alive")]
    KeepAlive { id: Uuid },
    #[serde(rename = "complete")]
    Complete {
        id: Uuid,
        errors: Option<Vec<graphql::Error>>,
    },
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "kind", rename = "lowercase")]
pub(crate) enum DeleteCallbackPayload {
    #[serde(rename = "subscription")]
    Subscription {
        id: Uuid,
        errors: Option<Vec<graphql::Error>>,
    },
}

#[derive(Clone)]
pub(crate) struct CallbackService {
    notify: Notify<Uuid, graphql::Response>,
    path: String,
}

impl CallbackService {
    pub(crate) fn new(notify: Notify<Uuid, graphql::Response>, path: String) -> Self {
        Self { notify, path }
    }
}

impl Service<router::Request> for CallbackService {
    type Response = router::Response;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, req: router::Request) -> Self::Future {
        let mut notify = self.notify.clone();
        let path = self.path.clone();
        Box::pin(async move {
            let (parts, body) = req.router_request.into_parts();
            let sub_id = match uuid::Uuid::from_str(
                parts.uri.path().trim_start_matches(&format!("{path}/")),
            ) {
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
                        CallbackPayload::Subscription(SubscriptionPayload::Next {
                            payload,
                            id,
                        }) => {
                            if let Some(res) = assert_ids(&req.context, &sub_id, &id) {
                                return Ok(res);
                            }
                            let mut handle = match notify.subscribe_if_exist(id).await {
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

                            handle.publish(id, payload).await?;

                            Ok(router::Response {
                                response: http::Response::builder()
                                    .status(StatusCode::OK)
                                    .body::<hyper::Body>("".into())
                                    .map_err(BoxError::from)?,
                                context: req.context,
                            })
                        }
                        CallbackPayload::Subscription(SubscriptionPayload::KeepAlive { id }) => {
                            if let Some(res) = assert_ids(&req.context, &sub_id, &id) {
                                return Ok(res);
                            }
                            if notify.exist(id).await? {
                                notify.keep_alive(id).await?;
                                Ok(router::Response {
                                    response: http::Response::builder()
                                        .status(StatusCode::NO_CONTENT)
                                        .body::<hyper::Body>("".into())
                                        .map_err(BoxError::from)?,
                                    context: req.context,
                                })
                            } else {
                                Ok(router::Response {
                                    response: http::Response::builder()
                                        .status(StatusCode::NOT_FOUND)
                                        .body("suscription doesn't exist".into())
                                        .map_err(BoxError::from)?,
                                    context: req.context,
                                })
                            }
                        }
                        CallbackPayload::Subscription(SubscriptionPayload::Init { id }) => {
                            if let Some(res) = assert_ids(&req.context, &sub_id, &id) {
                                return Ok(res);
                            }
                            if notify.exist(id).await? {
                                Ok(router::Response {
                                    response: http::Response::builder()
                                        .status(StatusCode::NO_CONTENT)
                                        .body::<hyper::Body>("".into())
                                        .map_err(BoxError::from)?,
                                    context: req.context,
                                })
                            } else {
                                Ok(router::Response {
                                    response: http::Response::builder()
                                        .status(StatusCode::NOT_FOUND)
                                        .body("suscription doesn't exist".into())
                                        .map_err(BoxError::from)?,
                                    context: req.context,
                                })
                            }
                        }
                        CallbackPayload::Subscription(SubscriptionPayload::Complete {
                            id,
                            errors,
                        }) => {
                            if let Some(res) = assert_ids(&req.context, &sub_id, &id) {
                                return Ok(res);
                            }
                            if let Some(errors) = errors {
                                let _ = notify
                                    .publish(
                                        id,
                                        graphql::Response::builder().errors(errors).build(),
                                    )
                                    .await;
                            }
                            notify.try_delete(id);
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

                    match cb_body {
                        DeleteCallbackPayload::Subscription { id, errors } => {
                            if let Some(res) = assert_ids(&req.context, &sub_id, &id) {
                                return Ok(res);
                            }

                            if let Some(errors) = errors {
                                let _ = notify
                                    .publish(
                                        id,
                                        graphql::Response::builder().errors(errors).build(),
                                    )
                                    .await;
                            }

                            notify.try_delete(id);
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

fn assert_ids(
    context: &Context,
    id_from_path: &Uuid,
    id_from_body: &Uuid,
) -> Option<router::Response> {
    if id_from_path != id_from_body {
        return Some(router::Response {
            response: http::Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body::<hyper::Body>("id from url path and id from body are different".into())
                .expect("this body is valid"),
            context: context.clone(),
        });
    }

    None
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use futures::StreamExt;
    use serde_json::Value;
    use tower::util::BoxService;
    use tower::Service;
    use tower::ServiceExt;

    use super::*;
    use crate::graphql::Request;
    use crate::http_ext;
    use crate::plugin::test::MockSubgraphService;
    use crate::plugin::DynPlugin;
    use crate::services::SubgraphRequest;
    use crate::services::SubgraphResponse;
    use crate::Notify;

    #[tokio::test(flavor = "multi_thread")]
    async fn it_test_callback_endpoint() {
        let mut notify = Notify::new();
        let dyn_plugin: Box<dyn DynPlugin> = crate::plugin::plugins()
            .find(|factory| factory.name == "apollo.subscription")
            .expect("Plugin not found")
            .create_instance(
                &Value::from_str(
                    r#"{
                "enabled": true,
                "mode": {
                    "callback": {
                        "public_url": "http://localhost:4000",
                        "path": "/subscription/callback"
                    }
                }
            }"#,
                )
                .unwrap(),
                Default::default(),
                notify.clone(),
            )
            .await
            .unwrap();

        let http_req_prom = http::Request::get("http://localhost:4000/subscription/callback")
            .body(Default::default())
            .unwrap();
        let mut web_endpoint = dyn_plugin
            .web_endpoints()
            .into_iter()
            .next()
            .unwrap()
            .1
            .into_iter()
            .next()
            .unwrap()
            .into_router();
        let resp = web_endpoint
            .ready()
            .await
            .unwrap()
            .call(http_req_prom)
            .await
            .unwrap();
        assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);
        let new_sub_id = uuid::Uuid::new_v4();
        let mut handler = notify.subscribe(new_sub_id).await.unwrap();

        let http_req = http::Request::post(format!(
            "http://localhost:4000/subscription/callback/{new_sub_id}"
        ))
        .body(hyper::Body::from(
            serde_json::to_vec(&CallbackPayload::Subscription(SubscriptionPayload::Init {
                id: new_sub_id,
            }))
            .unwrap(),
        ))
        .unwrap();
        let resp = web_endpoint.clone().oneshot(http_req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::NO_CONTENT);

        let http_req = http::Request::post(format!(
            "http://localhost:4000/subscription/callback/{new_sub_id}"
        ))
        .body(hyper::Body::from(
            serde_json::to_vec(&CallbackPayload::Subscription(SubscriptionPayload::Next {
                id: new_sub_id,
                payload: graphql::Response::builder()
                    .data(serde_json_bytes::json!({"userWasCreated": {"username": "ada_lovelace"}}))
                    .build(),
            }))
            .unwrap(),
        ))
        .unwrap();
        let resp = web_endpoint.clone().oneshot(http_req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let msg = handler.receiver().next().await.unwrap();

        assert_eq!(
            msg,
            graphql::Response::builder()
                .data(serde_json_bytes::json!({"userWasCreated": {"username": "ada_lovelace"}}))
                .build()
        );
        drop(handler);

        // Should answer NOT FOUND because I dropped the only existing handler and so no one is still listening to the sub
        let http_req = http::Request::post(format!(
            "http://localhost:4000/subscription/callback/{new_sub_id}"
        ))
        .body(hyper::Body::from(
            serde_json::to_vec(&CallbackPayload::Subscription(SubscriptionPayload::Next {
                id: new_sub_id,
                payload: graphql::Response::builder()
                    .data(serde_json_bytes::json!({"userWasCreated": {"username": "ada_lovelace"}}))
                    .build(),
            }))
            .unwrap(),
        ))
        .unwrap();
        let resp = web_endpoint.oneshot(http_req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_test_callback_endpoint_with_complete_subscription() {
        let mut notify = Notify::new();
        let dyn_plugin: Box<dyn DynPlugin> = crate::plugin::plugins()
            .find(|factory| factory.name == "apollo.subscription")
            .expect("Plugin not found")
            .create_instance(
                &Value::from_str(
                    r#"{
                "enabled": true,
                "mode": {
                    "callback": {
                        "public_url": "http://localhost:4000",
                        "path": "/subscription/callback"
                    }
                }
            }"#,
                )
                .unwrap(),
                Default::default(),
                notify.clone(),
            )
            .await
            .unwrap();

        let http_req_prom = http::Request::get("http://localhost:4000/subscription/callback")
            .body(Default::default())
            .unwrap();
        let mut web_endpoint = dyn_plugin
            .web_endpoints()
            .into_iter()
            .next()
            .unwrap()
            .1
            .into_iter()
            .next()
            .unwrap()
            .into_router();
        let resp = web_endpoint
            .ready()
            .await
            .unwrap()
            .call(http_req_prom)
            .await
            .unwrap();
        assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);
        let new_sub_id = uuid::Uuid::new_v4();
        let mut handler = notify.subscribe(new_sub_id).await.unwrap();

        let http_req = http::Request::post(format!(
            "http://localhost:4000/subscription/callback/{new_sub_id}"
        ))
        .body(hyper::Body::from(
            serde_json::to_vec(&CallbackPayload::Subscription(SubscriptionPayload::Init {
                id: new_sub_id,
            }))
            .unwrap(),
        ))
        .unwrap();
        let resp = web_endpoint.clone().oneshot(http_req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::NO_CONTENT);

        let http_req = http::Request::post(format!(
            "http://localhost:4000/subscription/callback/{new_sub_id}"
        ))
        .body(hyper::Body::from(
            serde_json::to_vec(&CallbackPayload::Subscription(SubscriptionPayload::Next {
                id: new_sub_id,
                payload: graphql::Response::builder()
                    .data(serde_json_bytes::json!({"userWasCreated": {"username": "ada_lovelace"}}))
                    .build(),
            }))
            .unwrap(),
        ))
        .unwrap();
        let resp = web_endpoint.clone().oneshot(http_req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let msg = handler.receiver().next().await.unwrap();

        assert_eq!(
            msg,
            graphql::Response::builder()
                .data(serde_json_bytes::json!({"userWasCreated": {"username": "ada_lovelace"}}))
                .build()
        );

        let http_req = http::Request::post(format!(
            "http://localhost:4000/subscription/callback/{new_sub_id}"
        ))
        .body(hyper::Body::from(
            serde_json::to_vec(&CallbackPayload::Subscription(
                SubscriptionPayload::Complete {
                    id: new_sub_id,
                    errors: Some(vec![graphql::Error::builder()
                        .message("cannot complete the subscription")
                        .extension_code("SUBSCRIPTION_ERROR")
                        .build()]),
                },
            ))
            .unwrap(),
        ))
        .unwrap();
        let resp = web_endpoint.clone().oneshot(http_req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::ACCEPTED);
        let msg = handler.receiver().next().await.unwrap();

        assert_eq!(
            msg,
            graphql::Response::builder()
                .errors(vec![graphql::Error::builder()
                    .message("cannot complete the subscription")
                    .extension_code("SUBSCRIPTION_ERROR")
                    .build()])
                .build()
        );

        // Should answer NOT FOUND because we completed the sub
        let http_req = http::Request::post(format!(
            "http://localhost:4000/subscription/callback/{new_sub_id}"
        ))
        .body(hyper::Body::from(
            serde_json::to_vec(&CallbackPayload::Subscription(SubscriptionPayload::Next {
                id: new_sub_id,
                payload: graphql::Response::builder()
                    .data(serde_json_bytes::json!({"userWasCreated": {"username": "ada_lovelace"}}))
                    .build(),
            }))
            .unwrap(),
        ))
        .unwrap();
        let resp = web_endpoint.oneshot(http_req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_test_subgraph_service_with_subscription_disabled() {
        let dyn_plugin: Box<dyn DynPlugin> = crate::plugin::plugins()
            .find(|factory| factory.name == "apollo.subscription")
            .expect("Plugin not found")
            .create_instance(
                &Value::from_str(r#"{}"#).unwrap(),
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();

        let mut mock_subgraph_service = MockSubgraphService::new();
        mock_subgraph_service
            .expect_call()
            .times(0)
            .returning(move |req: SubgraphRequest| {
                Ok(SubgraphResponse::fake_builder()
                    .context(req.context)
                    .build())
            });

        let mut subgraph_service =
            dyn_plugin.subgraph_service("my_subgraph_name", BoxService::new(mock_subgraph_service));
        let subgraph_req = SubgraphRequest::fake_builder()
            .subgraph_request(
                http_ext::Request::fake_builder()
                    .body(
                        Request::fake_builder()
                            .query(String::from(
                                "subscription {\n  userWasCreated {\n    username\n  }\n}",
                            ))
                            .build(),
                    )
                    .build()
                    .unwrap(),
            )
            .operation_kind(OperationKind::Subscription)
            .build();
        let subgraph_response = subgraph_service
            .ready()
            .await
            .unwrap()
            .call(subgraph_req)
            .await
            .unwrap();

        assert_eq!(subgraph_response.response.body(), &graphql::Response::builder().data(serde_json_bytes::Value::Null).error(graphql::Error::builder().message("cannot execute a subscription if it's not enabled in the configuration").extension_code("SUBSCRIPTION_DISABLED").build()).extensions(Object::default()).build());
    }
}

register_plugin!("apollo", "subscription", Subscription);
