use apollo_router::plugin::test::MockSubgraph;
use apollo_router::services::supergraph;
use apollo_router::MockedSubgraphs;
use apollo_router::TestHarness;
use serde_json::json;

use tower::{Service, ServiceExt};

static QUERY: &str = r#"query TopProducts($first: Int) { topProducts(first: $first) { upc name reviews { id product { name } author { id name } } } }"#;

pub fn setup(json_config: serde_json::Value) -> TestHarness<'static> {
    let account_service =  MockSubgraph::builder().with_json(json!{{
        "query": "query TopProducts__accounts__3($representations:[_Any!]!){_entities(representations:$representations){...on User{name}}}",
        "operationName": "TopProducts__accounts__3",
        "variables": {
            "representations": [
                {
                    "__typename": "User",
                    "id": "1"
                },
                {
                    "__typename": "User",
                    "id": "2"
                },
                {
                    "__typename": "User",
                    "id": "1"
                }
            ]
        }
    }},
    json!{{
        "errors": [
            {
                "message": "this is an error message",
                "extensions": {
                    "code": "THIS_IS_AN_ERROR_CODE",
                    "serviceName": "accounts i guess",
                    "exception": {
                        "code": "EXP_001",
                        "message": "this is an error message",
                        "stacktrace": [
                            "this is a first file and line number",
                            "this is an other one",
                            "and an other one",
                            "and it keeps on going"
                        ]
                    }
                }
            }
        ]
    }}).build();

    let review_service = MockSubgraph::builder().with_json(json!{{
        "query": "query TopProducts__reviews__1($representations:[_Any!]!){_entities(representations:$representations){...on Product{reviews{id product{__typename upc}author{__typename id}}}}}",
        "operationName": "TopProducts__reviews__1",
        "variables": {
            "representations":[
                {
                    "__typename": "Product",
                    "upc":"1"
                },
                {
                    "__typename": "Product",
                    "upc": "2"
                }
            ]
        }
    }},
    json!{{
        "data": {
            "_entities": [
                {
                    "reviews": [
                        {
                            "id": "1",
                            "product": {
                                "__typename": "Product",
                                "upc": "1"
                            },
                            "author": {
                                "__typename": "User",
                                "id": "1"
                            }
                        },
                        {
                            "id": "4",
                            "product": {
                                "__typename": "Product",
                                "upc": "1"
                            },
                            "author": {
                                "__typename": "User",
                                "id": "2"
                            }
                        }
                    ]
                },
                {
                    "reviews": [
                        {
                            "id": "2",
                            "product": {
                                "__typename": "Product",
                                "upc": "2"
                            },
                            "author": {
                                "__typename": "User",
                                "id": "1"
                            }
                        }
                    ]
                }
            ]
        }
    }}).build();

    let product_service =  MockSubgraph::builder().with_json(json!{{
        "query": "query TopProducts__products__0($first:Int){topProducts(first:$first){__typename upc name}}",
        "operationName": "TopProducts__products__0",
        "variables":{
            "first":2u8
        },
    }},
    json!{{
        "data": {
            "topProducts": [
                {
                    "__typename": "Product",
                    "upc": "1",
                    "name":"Table"
                },
                {
                    "__typename": "Product",
                    "upc": "2",
                    "name": "Couch"
                }
            ]
        }
    }}).with_json(  json!{{
        "query": "query TopProducts__products__2($representations:[_Any!]!){_entities(representations:$representations){...on Product{name}}}",
        "operationName": "TopProducts__products__2",
        "variables": {
            "representations": [
                {
                    "__typename": "Product",
                    "upc": "1"
                },
                {
                    "__typename": "Product",
                    "upc": "1"
                },
                {
                    "__typename": "Product",
                    "upc": "2"
                }
            ]
        }
    }},
    json!{{
        "data": {
            "_entities": [
                {
                    "name": "Table"
                },
                {
                    "name": "Table"
                },
                {
                    "name": "Couch"
                }
            ]
        }
    }}).build();
    let mut mocks = MockedSubgraphs::default();
    mocks.insert("accounts", account_service);
    mocks.insert("reviews", review_service);
    mocks.insert("products", product_service);

    let schema = include_str!("fixtures/supergraph.graphql");
    TestHarness::builder()
        .schema(schema)
        .configuration_json(json_config)
        .unwrap()
        .extra_plugin(mocks)
}

#[tokio::test]
async fn redacted_errors_test() {
    let mut supergraph_service = setup(json! {{}}).build().await.unwrap();

    let request = supergraph::Request::fake_builder()
        .query(QUERY.to_string())
        .variable("first", 2usize)
        .build()
        .expect("expecting valid request");

    let redacted_response = supergraph_service
        .ready()
        .await
        .unwrap()
        .call(request)
        .await
        .unwrap()
        .next_response()
        .await
        .unwrap();

    println!(
        "{}",
        serde_json::to_string_pretty(&redacted_response).unwrap()
    );

    let redacted_errors: Vec<apollo_router::graphql::Error> = serde_json::from_str(
        r#"[
    {
      "message": "Subgraph errors redacted"
    },
    {
      "message": "Subgraph response from 'accounts' was missing key `_entities`",
      "path": [
        "topProducts",
        "@",
        "reviews",
        "@",
        "author"
      ]
    }
  ]"#,
    )
    .unwrap();

    assert_eq!(redacted_response.errors, redacted_errors);
}

#[tokio::test]
async fn not_redacted_errors_test() {
    let mut supergraph_service = setup(json! {{ "include_subgraph_errors": { "all": true}}})
        .build()
        .await
        .unwrap();

    let request = supergraph::Request::fake_builder()
        .query(QUERY.to_string())
        .variable("first", 2usize)
        .build()
        .expect("expecting valid request");

    let non_redacted_response = supergraph_service
        .ready()
        .await
        .unwrap()
        .call(request)
        .await
        .unwrap()
        .next_response()
        .await
        .unwrap();

    println!(
        "{}",
        serde_json::to_string_pretty(&non_redacted_response).unwrap()
    );

    let non_redacted_errors: Vec<apollo_router::graphql::Error> = serde_json::from_str(
        r#"[
            {
              "message": "this is an error message",
              "extensions": {
                "code": "THIS_IS_AN_ERROR_CODE",
                "serviceName": "accounts i guess",
                "exception": {
                  "code": "EXP_001",
                  "message": "this is an error message",
                  "stacktrace": [
                    "this is a first file and line number",
                    "this is an other one",
                    "and an other one",
                    "and it keeps on going"
                  ]
                }
              }
            },
            {
              "message": "Subgraph response from 'accounts' was missing key `_entities`",
              "path": [
                "topProducts",
                "@",
                "reviews",
                "@",
                "author"
              ]
            }
          ]"#,
    )
    .unwrap();

    assert_eq!(non_redacted_response.errors, non_redacted_errors);
}
