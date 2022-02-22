use apollo_router_core::plugin_utils;
use apollo_router_core::prelude::*;
use apollo_router_core::register_plugin;
use apollo_router_core::ExecutionRequest;
use apollo_router_core::ExecutionResponse;
use apollo_router_core::Plugin;
use apollo_router_core::Schema;
use apollo_router_core::SubgraphRequest;
use apollo_router_core::SubgraphResponse;
use futures::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::task::Poll;
use tower::util::BoxService;
use tower::BoxError;
use tower::Layer;
use tower::Service;
use tower::ServiceExt;

#[derive(Clone)]
struct MockSubgraph {
    schema: Arc<Schema>,
    services: Vec<String>,
}

impl Service<SubgraphRequest> for MockSubgraph {
    type Response = SubgraphResponse;

    type Error = BoxError;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: SubgraphRequest) -> Self::Future {
        let apollo_router_core::Request {
            query, variables, ..
        } = req.http_request.body();

        let as_graphql = apollo_router_core::Query::parse(query.clone().unwrap())
            .expect("hope we checked it before tbh");

        let data = as_graphql.generate_response(&self.schema);

        dbg!(&data);
        Box::pin(async {
            Ok(plugin_utils::SubgraphResponse::builder()
                .data(data)
                .context(req.context)
                .build()
                .into())
        })
    }
}

struct PreventMutationsLayer {}

pub struct PreventMutationsService<S>
where
    S: Service<ExecutionRequest, Response = ExecutionResponse> + Send,
    <S as Service<ExecutionRequest>>::Future: Send + 'static,
{
    service: S,
}

impl<S> Layer<S> for PreventMutationsLayer
where
    S: Service<ExecutionRequest, Response = ExecutionResponse> + Send,
    <S as Service<ExecutionRequest>>::Future: Send + 'static,
{
    type Service = PreventMutationsService<S>;

    fn layer(&self, service: S) -> Self::Service {
        PreventMutationsService { service }
    }
}

impl<S> Service<ExecutionRequest> for PreventMutationsService<S>
where
    S: Service<ExecutionRequest, Response = ExecutionResponse> + Send,
    <S as Service<ExecutionRequest>>::Future: Send,
{
    type Response = ExecutionResponse;

    type Error = S::Error;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ExecutionRequest) -> Self::Future {
        if req.query_plan.contains_mutations() {
            let res = plugin_utils::ExecutionResponse::builder()
                .errors(vec![apollo_router_core::Error {
                    message: "MockSubgraphPlugin only accepts queries".to_string(),
                    locations: Default::default(),
                    path: Default::default(),
                    extensions: Default::default(),
                }])
                .context(req.context)
                .build()
                .into();

            Box::pin(async { Ok(res) })
        } else {
            Box::pin(self.service.call(req))
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct SchemaConfig {
    path: PathBuf,
    #[serde(default)]
    services: Vec<String>,
}

impl Plugin for MockSubgraph {
    type Config = SchemaConfig;

    fn new(configuration: Self::Config) -> Result<Self, BoxError> {
        let Self::Config { services, path } = configuration;

        let current_directory = std::env::current_dir()?;
        let schema = Arc::new(graphql::Schema::read(&current_directory.join(path))?);
        tracing::info!("Mock subgraph plugin is set up!");
        Ok(Self { schema, services })
    }

    fn execution_service(
        &mut self,
        service: BoxService<ExecutionRequest, ExecutionResponse, BoxError>,
    ) -> BoxService<ExecutionRequest, ExecutionResponse, BoxError> {
        PreventMutationsLayer {}.layer(service).boxed()
    }
    fn subgraph_service(
        &mut self,
        name: &str,
        service: BoxService<SubgraphRequest, SubgraphResponse, BoxError>,
    ) -> BoxService<SubgraphRequest, SubgraphResponse, BoxError> {
        if self.services.is_empty() || self.services.contains(&name.to_string()) {
            self.clone().boxed()
        } else {
            service
        }
    }
}

register_plugin!("apollographql", "mock-subgraph", MockSubgraph);

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use std::str::FromStr;

    #[tokio::test]
    async fn layer_registered() {
        apollo_router_core::plugins()
            .get("apollographql.mock-subgraph")
            .expect("Plugin not found")
            .create_instance(&Value::from_str("{\"path\":\"supergraph.graphql\"}").unwrap())
            .unwrap();
    }
}
