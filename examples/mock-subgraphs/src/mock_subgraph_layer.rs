use apollo_router_core::register_layer;
use apollo_router_core::ConfigurableLayer;
use apollo_router_core::Schema;
use schemars::JsonSchema;
use serde::Deserialize;
use tower::layer::layer_fn;
use tower::{BoxError, Layer};

struct MockSubgraph {
    schema: Schema,
}

impl<S> Layer<S> for MockSubgraph {
    type Service = S;

    fn layer(&self, inner: S) -> Self::Service {
        layer_fn(|s| s).layer(inner)
    }
}

impl MockSubgraph {
    fn new(schema: Schema) -> Result<Self, BoxError> {
        tracing::info!("Mock subgraph layer is set up!");
        Ok(Self { schema })
    }
}

register_layer!("apollographql.com", "mock-subgraph", MockSubgraph);

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use std::str::FromStr;

    #[tokio::test]
    async fn layer_registered() {
        apollo_router_core::layers()
            .get("example.com_hello")
            .expect("Layer not found")
            .create_instance(&Value::from_str("{\"name\":\"Bob\"}").unwrap())
            .unwrap();
    }
}
