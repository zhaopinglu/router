use apollo_router_core::prelude::*;
use apollo_router_core::register_plugin;
use apollo_router_core::Plugin;
use apollo_router_core::Schema;
use apollo_router_core::SubgraphRequest;
use apollo_router_core::SubgraphResponse;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tower::util::BoxService;
use tower::BoxError;
use tower::ServiceExt;

struct MockSubgraph {
    schema: Schema,
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct SchemaConfig {
    path: PathBuf,
}

impl Plugin for MockSubgraph {
    type Config = SchemaConfig;

    fn new(configuration: Self::Config) -> Result<Self, BoxError> {
        let current_directory = std::env::current_dir()?;
        let schema = graphql::Schema::read(&current_directory.join(configuration.path))?;
        tracing::info!("Mock subgraph plugin is set up!");
        Ok(Self { schema })
    }

    fn subgraph_service(
        &mut self,
        name: &str,
        service: BoxService<SubgraphRequest, SubgraphResponse, BoxError>,
    ) -> BoxService<SubgraphRequest, SubgraphResponse, BoxError> {
        let name = name.to_string();
        service
            .map_request(move |req| {
                tracing::info!("called on {}", name);
                req
            })
            .boxed()
    }
}

register_plugin!("apollographql.com", "mock-subgraph", MockSubgraph);

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use std::str::FromStr;

    #[tokio::test]
    async fn layer_registered() {
        apollo_router_core::plugins()
            .get("apollographql.com_mock-subgraph")
            .expect("Plugin not found")
            .create_instance(&Value::from_str("{\"path\":\"supergraph.graphql\"}").unwrap())
            .unwrap();
    }
}
