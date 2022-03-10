use apollo_router_core::Schema;
use criterion::{criterion_group, criterion_main, Criterion};
use once_cell::sync::Lazy;
use router_bridge::plan::{self, BridgePlanner};
use serde_json::json;
use std::sync::Arc;

static EXPECTED_PLAN: Lazy<serde_json::Value> = Lazy::new(
    || json!({"kind":"QueryPlan","node":{"kind":"Sequence","nodes":[{"kind":"Fetch","serviceName":"products","variableUsages":["first"],"operation":"query($first:Int){topProducts(first:$first){__typename upc name}}","operationKind":"query"},{"kind":"Flatten","path":["topProducts","@"],"node":{"kind":"Fetch","serviceName":"reviews","requires":[{"kind":"InlineFragment","typeCondition":"Product","selections":[{"kind":"Field","name":"__typename"},{"kind":"Field","name":"upc"}]}],"variableUsages":[],"operation":"query($representations:[_Any!]!){_entities(representations:$representations){...on Product{reviews{id product{__typename upc}author{__typename id}}}}}","operationKind":"query"}},{"kind":"Parallel","nodes":[{"kind":"Flatten","path":["topProducts","@","reviews","@","product"],"node":{"kind":"Fetch","serviceName":"products","requires":[{"kind":"InlineFragment","typeCondition":"Product","selections":[{"kind":"Field","name":"__typename"},{"kind":"Field","name":"upc"}]}],"variableUsages":[],"operation":"query($representations:[_Any!]!){_entities(representations:$representations){...on Product{name}}}","operationKind":"query"}},{"kind":"Flatten","path":["topProducts","@","reviews","@","author"],"node":{"kind":"Fetch","serviceName":"accounts","requires":[{"kind":"InlineFragment","typeCondition":"User","selections":[{"kind":"Field","name":"__typename"},{"kind":"Field","name":"id"}]}],"variableUsages":[],"operation":"query($representations:[_Any!]!){_entities(representations:$representations){...on User{name}}}","operationKind":"query"}}]}]}}),
);

static QUERY: &str = r#"query TopProducts($first: Int) { topProducts(first: $first) { upc name reviews { id product { name } author { id name } } } }"#;

fn plan_query(schema: Arc<Schema>) {
    let context = plan::OperationalContext {
        schema: schema.as_str().to_string(),
        query: QUERY.to_string(),
        operation_name: "".to_string(),
    };

    let plan = plan::plan::<serde_json::Value>(
        context,
        plan::QueryPlanOptions {
            auto_fragmentization: false,
        },
    )
    .unwrap()
    .unwrap();

    assert_eq!(&*EXPECTED_PLAN, &plan);
}

fn plan_query_v2(planner: BridgePlanner) {
    let context = plan::OperationalContext2 {
        query: QUERY.to_string(),
        operation_name: "".to_string(),
    };

    let plan = planner.plan::<serde_json::Value>(context).unwrap().unwrap();

    assert_eq!(&*EXPECTED_PLAN, &plan);
}

fn query_plan_v1(c: &mut Criterion) {
    let schema: Arc<Schema> =
        Arc::new(include_str!("fixtures/supergraph.graphql").parse().unwrap());

    c.bench_function("query_planning_v1", move |b| {
        b.iter(|| plan_query(schema.clone()));
    });
}

fn query_plan_v2(c: &mut Criterion) {
    let schema: Arc<String> = Arc::new(include_str!("fixtures/supergraph.graphql").to_string());

    let planner = BridgePlanner::new(schema).unwrap();

    c.bench_function("query_planning_v2", move |b| {
        b.iter(|| plan_query_v2(planner.clone()));
    });
}

criterion_group!(benches, query_plan_v2, query_plan_v1);
criterion_main!(benches);
