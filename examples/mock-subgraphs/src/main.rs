//! Main entry point for CLI command to start server.

use anyhow::Result;
use apollo_router::ApolloRouterBuilder;
use apollo_router::{ConfigurationKind, SchemaKind, ShutdownKind, State};
use futures::prelude::*;
use tracing_subscriber::EnvFilter;

mod mock_subgraph_plugin;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt::fmt()
        .with_env_filter(EnvFilter::try_new("info").expect("could not parse log"))
        .init();

    let current_directory = std::env::current_dir()?;

    let schema = SchemaKind::File {
        path: current_directory.join("supergraph.graphql"),
        watch: false,
        delay: None,
    };

    let configuration = ConfigurationKind::File {
        path: current_directory.join("config.yml"),
        watch: false,
        delay: None,
    };

    let server = ApolloRouterBuilder::default()
        .configuration(configuration)
        .schema(schema)
        .shutdown(ShutdownKind::CtrlC)
        .build();

    let mut server_handle = server.serve();
    server_handle
        .state_receiver()
        .for_each(|state| {
            match state {
                State::Startup => {
                    tracing::info!(r#"Starting Apollo Router"#)
                }
                State::Running { address, .. } => {
                    tracing::info!("Listening on {} 🚀", address)
                }
                State::Stopped => {
                    tracing::info!("Stopped")
                }
                State::Errored => {
                    tracing::info!("Stopped with error")
                }
            }
            future::ready(())
        })
        .await;

    if let Err(err) = server_handle.await {
        tracing::error!("{}", err);
        return Err(err.into());
    }

    Ok(())
}
