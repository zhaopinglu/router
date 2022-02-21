use apollo_router_core::plugin_utils;
use apollo_router_core::prelude::*;
use apollo_router_core::register_plugin;
use apollo_router_core::Plugin;
use apollo_router_core::Schema;
use apollo_router_core::SubgraphRequest;
use apollo_router_core::SubgraphResponse;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tower::util::BoxService;
use tower::BoxError;
use tower::ServiceExt;

struct MockSubgraph {
    schema: Arc<Schema>,
    services: Vec<String>,
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

    fn subgraph_service(
        &mut self,
        name: &str,
        service: BoxService<SubgraphRequest, SubgraphResponse, BoxError>,
    ) -> BoxService<SubgraphRequest, SubgraphResponse, BoxError> {
        let mut mock_service = plugin_utils::MockSubgraphService::new();
        let schema_clone = Arc::clone(&self.schema);
        mock_service
            .expect_call()
            .returning(move |req: SubgraphRequest| {
                let apollo_router_core::Request {
                    query, variables, ..
                } = req.http_request.body();

                let as_graphql = apollo_router_core::Query::parse(query.clone().unwrap())
                    .expect("hope we checked it before tbh");

                let data = as_graphql.generate_response(&schema_clone);

                Ok(plugin_utils::SubgraphResponse::builder()
                    .data(data)
                    .context(req.context)
                    .build()
                    .into())
            });

        let mock = mock_service.build();

        if self.services.is_empty() || self.services.contains(&name.to_string()) {
            mock.boxed()
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
