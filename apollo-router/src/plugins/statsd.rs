use crate::plugin::Plugin;
use crate::{register_plugin, SubgraphRequest, SubgraphResponse};
use cadence::prelude::*;
use cadence::{StatsdClient, UdpMetricSink, DEFAULT_PORT};
use schemars::JsonSchema;
use serde::Deserialize;
use std::net::UdpSocket;
use tower::util::BoxService;
use tower::{BoxError, ServiceBuilder, ServiceExt};

#[derive(Debug)]
struct Statsd {
    #[allow(dead_code)]
    configuration: Conf,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct Conf {
    // Put your plugin configuration here. It will automatically be deserialized from JSON.
    // Always put some sort of config here, even if it is just a bool to say that the plugin is enabled,
    // otherwise the yaml to enable the plugin will be confusing.
    message: String,
}
// This is a bare bones plugin that can be duplicated when creating your own.
#[async_trait::async_trait]
impl Plugin for Statsd {
    type Config = Conf;

    async fn new(configuration: Self::Config) -> Result<Self, BoxError> {
        tracing::info!("{}", configuration.message);
        Ok(Statsd { configuration })
    }

    // Delete this function if you are not customizing it.
    fn subgraph_service(
        &mut self,
        _name: &str,
        service: BoxService<SubgraphRequest, SubgraphResponse, BoxError>,
    ) -> BoxService<SubgraphRequest, SubgraphResponse, BoxError> {
        ServiceBuilder::new()
            .map_request(|req: SubgraphRequest| {
                // XXX Do Something
                req
            })
            .service(service)
            .map_response(|resp: SubgraphResponse| {
                // XXX Do Something
                let host = ("localhost", DEFAULT_PORT);
                let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
                let sink = UdpMetricSink::from(host, socket).unwrap();
                let client = StatsdClient::from_sink("router.metrics", sink);
                if let Err(err) = client.incr("subgraph.requests") {
                    tracing::error!(%err, "subgraph.requests increment");
                }
                resp
            })
            .boxed()
    }
}

// This macro allows us to use it in our plugin registry!
// register_plugin takes a group name, and a plugin name.
register_plugin!("statsd", "statsd", Statsd);

#[cfg(test)]
mod tests {
    use super::{Conf, Statsd};

    use apollo_router::utils::test::IntoSchema::Canned;
    use apollo_router::utils::test::PluginTestHarness;
    use apollo_router::{Plugin, ResponseBody};
    use tower::BoxError;

    #[tokio::test]
    async fn plugin_registered() {
        apollo_router::plugins()
            .get("statsd.statsd")
            .expect("Plugin not found")
            .create_instance(&serde_json::json!({"message" : "Starting my plugin"}))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn basic_test() -> Result<(), BoxError> {
        // Define a configuration to use with our plugin
        let conf = Conf {
            message: "Starting my plugin".to_string(),
        };

        // Build an instance of our plugin to use in the test harness
        let plugin = Statsd::new(conf).await.expect("created plugin");

        // Create the test harness. You can add mocks for individual services, or use prebuilt canned services.
        let mut test_harness = PluginTestHarness::builder()
            .plugin(plugin)
            .schema(Canned)
            .build()
            .await?;

        // Send a request
        let result = test_harness.call_canned().await?;
        if let ResponseBody::GraphQL(graphql) = result.response.body() {
            assert!(graphql.data.is_some());
        } else {
            panic!("expected graphql response")
        }

        Ok(())
    }
}
