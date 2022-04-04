use apollo_router_core::{prelude::*, RouterBridgeQueryPlanner};
use std::sync::Arc;

#[test]
fn test_starstuff_supergraph_is_valid() {
    include_str!("../../examples/graphql/supergraph.graphql")
        .parse::<graphql::Schema>()
        .expect(
            r#"Couldn't parse the supergraph example.
This file is being used in the router documentation, as a quickstart example.
Make sure it is accessible, and the configuration is working with the router."#,
        );

    insta::assert_snapshot!(include_str!("../../examples/graphql/supergraph.graphql"));
}

#[tokio::test]
async fn test_query_planning_did_not_change() {
    let schema = Arc::new(
        include_str!("../../examples/graphql/supergraph.graphql")
            .parse::<graphql::Schema>()
            .unwrap(),
    );

    let query = r#"query TopProducts($first: Int) { topProducts(first: $first) { upc name reviews { id product { name } author { id name } } } }"#;

    let planner = RouterBridgeQueryPlanner::new(schema);
    let query_plan = planner
        .get(query.into(), None, Default::default())
        .await
        .unwrap();

    // Using println!() here so it shows up if the test fails (insta doesn't support messages in assert!)
    println!("/!\\ If this test fails, you want to double check the benchmark mocks, and run the benchmarks locally /!\\");
    insta::assert_debug_snapshot!(query_plan);
}
