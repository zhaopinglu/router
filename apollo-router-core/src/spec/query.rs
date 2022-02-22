use crate::prelude::graphql::*;
use apollo_parser::ast;
use derivative::Derivative;
use serde_json_bytes::ByteString;
use std::collections::{HashMap, HashSet};
use tracing::level_filters::LevelFilter;

#[derive(Debug, Derivative)]
#[derivative(PartialEq, Hash, Eq)]
pub struct Query {
    string: String,
    #[derivative(PartialEq = "ignore", Hash = "ignore")]
    fragments: Fragments,
    #[derivative(PartialEq = "ignore", Hash = "ignore")]
    operations: Vec<Operation>,
    #[derivative(PartialEq = "ignore", Hash = "ignore")]
    operation_type_map: HashMap<OperationType, String>,
}

impl Query {
    /// Returns a reference to the underlying query string.
    pub fn as_str(&self) -> &str {
        self.string.as_str()
    }

    /// Re-format the response value to match this query.
    ///
    /// This will discard unrequested fields and re-order the output to match the order of the
    /// query.
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn format_response(
        &self,
        response: &mut Response,
        operation_name: Option<&str>,
        schema: &Schema,
    ) {
        let data = std::mem::take(&mut response.data);
        match data {
            Value::Object(init) => {
                let output = self.operations.iter().fold(init, |mut input, operation| {
                    if operation_name.is_none() || operation.name.as_deref() == operation_name {
                        let mut output = Object::default();
                        self.apply_selection_set(
                            &operation.selection_set,
                            &mut input,
                            &mut output,
                            schema,
                        );
                        output
                    } else {
                        input
                    }
                });
                response.data = output.into();
            }
            _ => {
                failfast_debug!("Invalid type for data in response.");
            }
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    pub fn parse(query: impl Into<String>) -> Option<Self> {
        let string = query.into();

        let parser = apollo_parser::Parser::new(string.as_str());
        let tree = parser.parse();
        let errors = tree
            .errors()
            .map(|err| format!("{:?}", err))
            .collect::<Vec<_>>();

        if !errors.is_empty() {
            failfast_debug!("Parsing error(s): {}", errors.join(", "));
            return None;
        }

        let document = tree.document();
        let fragments = Fragments::from(&document);

        let operations = document
            .definitions()
            .filter_map(|definition| {
                if let ast::Definition::OperationDefinition(operation) = definition {
                    Some(operation.into())
                } else {
                    None
                }
            })
            .collect();

        let operation_type_map = document
            .definitions()
            .filter_map(|definition| match definition {
                ast::Definition::SchemaDefinition(definition) => {
                    Some(definition.root_operation_type_definitions())
                }
                ast::Definition::SchemaExtension(extension) => {
                    Some(extension.root_operation_type_definitions())
                }
                _ => None,
            })
            .flatten()
            .map(|definition| {
                // Spec: https://spec.graphql.org/draft/#sec-Schema
                let type_name = definition
                    .named_type()
                    .expect("the node NamedType is not optional in the spec; qed")
                    .name()
                    .expect("the node Name is not optional in the spec; qed")
                    .text()
                    .to_string();
                let operation_type = OperationType::from(
                    definition
                        .operation_type()
                        .expect("the node NamedType is not optional in the spec; qed"),
                );
                (operation_type, type_name)
            })
            .collect();

        Some(Query {
            string,
            fragments,
            operations,
            operation_type_map,
        })
    }

    fn apply_selection_set(
        &self,
        selection_set: &[Selection],
        input: &mut Object,
        output: &mut Object,
        schema: &Schema,
    ) {
        for selection in selection_set {
            match selection {
                Selection::Field {
                    name,
                    selection_set,
                } => {
                    if let Some((field_name, input_value)) = input.remove_entry(name.as_str()) {
                        if let Some(selection_set) = selection_set {
                            match input_value {
                                Value::Object(mut input_object) => {
                                    let mut output_object = Object::default();
                                    self.apply_selection_set(
                                        selection_set,
                                        &mut input_object,
                                        &mut output_object,
                                        schema,
                                    );
                                    output.insert(field_name, output_object.into());
                                }
                                Value::Array(input_array) => {
                                    let output_array = input_array
                                        .into_iter()
                                        .enumerate()
                                        .map(|(i, mut element)| {
                                            if let Some(input_object) = element.as_object_mut() {
                                                let mut output_object = Object::default();
                                                self.apply_selection_set(
                                                    selection_set,
                                                    input_object,
                                                    &mut output_object,
                                                    schema,
                                                );
                                                output_object.into()
                                            } else {
                                                failfast_debug!(
                                                    "Array element is not an object: {}[{}]",
                                                    name,
                                                    i,
                                                );
                                                element
                                            }
                                        })
                                        .collect::<Value>();
                                    output.insert(field_name, output_array);
                                }
                                _ => {
                                    output.insert(field_name, input_value);
                                    failfast_debug!(
                                        "Field is not an object nor an array of object: {}",
                                        name,
                                    );
                                }
                            }
                        } else {
                            output.insert(field_name, input_value);
                        }
                    } else {
                        failfast_debug!("Missing field: {}", name);
                    }
                }
                Selection::InlineFragment {
                    fragment:
                        Fragment {
                            type_condition,
                            selection_set,
                        },
                } => {
                    if let Some(typename) = input.get("__typename") {
                        if typename.as_str() == Some(type_condition.as_str()) {
                            self.apply_selection_set(selection_set, input, output, schema);
                        }
                    }
                }
                Selection::FragmentSpread { name } => {
                    if let Some(fragment) = self
                        .fragments
                        .get(name)
                        .or_else(|| schema.fragments.get(name))
                    {
                        if let Some(typename) = input.get("__typename") {
                            if typename.as_str() == Some(fragment.type_condition.as_str()) {
                                self.apply_selection_set(
                                    &fragment.selection_set,
                                    input,
                                    output,
                                    schema,
                                );
                            }
                        }
                    } else {
                        failfast_debug!("Missing fragment named: {}", name);
                    }
                }
            }
        }
    }

    /// Validate a [`Request`]'s variables against this [`Query`] using a provided [`Schema`].
    #[tracing::instrument(skip_all, level = "trace")]
    pub fn validate_variables(&self, request: &Request, schema: &Schema) -> Result<(), Response> {
        let operation_name = request.operation_name.as_deref();
        let operation_variable_types =
            self.operations
                .iter()
                .fold(HashMap::new(), |mut acc, operation| {
                    if operation_name.is_none() || operation.name.as_deref() == operation_name {
                        acc.extend(operation.variables.iter().map(|(k, v)| (k.as_str(), v)))
                    }
                    acc
                });

        if LevelFilter::current() >= LevelFilter::DEBUG {
            let known_variables = operation_variable_types.keys().cloned().collect();
            let provided_variables = request
                .variables
                .keys()
                .map(|k| k.as_str())
                .collect::<HashSet<_>>();
            let unknown_variables = provided_variables
                .difference(&known_variables)
                .collect::<Vec<_>>();
            if !unknown_variables.is_empty() {
                failfast_debug!(
                    "Received variable unknown to the query: {:?}",
                    unknown_variables,
                );
            }
        }

        let errors = operation_variable_types
            .iter()
            .filter_map(|(name, ty)| {
                let value = request.variables.get(*name).unwrap_or(&Value::Null);
                ty.validate_value(value, schema).err().map(|_| {
                    FetchError::ValidationInvalidTypeVariable {
                        name: name.to_string(),
                    }
                    .to_graphql_error(None)
                })
            })
            .collect::<Vec<_>>();

        if errors.is_empty() {
            Ok(())
        } else {
            Err(Response::builder().errors(errors).build())
        }
    }

    pub fn generate_response(&self, schema: &Schema) -> serde_json_bytes::Value {
        let mut output = Default::default();
        for operation in self.operations.iter() {
            self.do_stuff(
                &operation.selection_set,
                &mut output,
                schema,
                &FieldOrObjectType::Object(
                    schema
                        .object_types
                        .get("Query")
                        .expect("schema must contain queries; qed"),
                ),
                Path::empty(),
            )
        }

        output.into()
    }

    fn do_stuff(
        &self,
        parent_selection_set: &[Selection],
        output: &mut serde_json_bytes::Value,
        schema: &Schema,
        field_or_object_type: &FieldOrObjectType,
        parent_path: Path,
    ) {
        if let FieldOrObjectType::Field(FieldType::List(list_content)) = field_or_object_type {
            let len = 5; // TODO: Grab from variables or generate randomly
            let values = (0usize..len)
                .map(|_| {
                    // list selections take the parent's set
                    let mut object: serde_json_bytes::Value = Default::default();
                    self.do_stuff(
                        &parent_selection_set.to_vec(),
                        &mut object,
                        schema,
                        &FieldOrObjectType::Field(list_content),
                        parent_path.clone(),
                    );
                    serde_json_bytes::to_value(&object).unwrap()
                })
                .collect();
            let value_with_path = Value::from_path(&parent_path, values);

            output.deep_merge(value_with_path);
            return;
        }

        if let FieldOrObjectType::Field(FieldType::Named(type_name)) = field_or_object_type {
            let child_type = schema
                .object_types
                .get(type_name.as_str())
                .clone()
                .expect("this cannot happen on an already validated query; qed");

            let mut sub_value = Default::default();
            for (selection_set, field_type) in
                child_type.fields.iter().filter_map(|(name, field_type)| {
                    let selection_set = parent_selection_set
                        .iter()
                        .filter(|selection| match selection {
                            Selection::Field {
                                name: field_name, ..
                            } => field_name == name,
                            Selection::FragmentSpread { name: spread_name } => spread_name == name,
                            _ => false,
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    if selection_set.is_empty() {
                        None
                    } else {
                        Some((selection_set, field_type))
                    }
                })
            {
                self.do_stuff(
                    selection_set.as_slice(),
                    &mut sub_value,
                    schema,
                    &FieldOrObjectType::Field(field_type),
                    parent_path.clone(),
                );
            }
            let value_with_path = Value::from_path(&parent_path, sub_value);
            output.deep_merge(value_with_path);

            return;
        }

        for selection in parent_selection_set {
            match selection {
                Selection::Field {
                    name,
                    selection_set,
                } => {
                    let path = parent_path.join(Path::from_slice(&[name]));

                    let sub_value = match &field_or_object_type {
                        FieldOrObjectType::Field(FieldType::Boolean) => {
                            Value::from_path(&path, true.into())
                        }
                        FieldOrObjectType::Field(FieldType::String) => {
                            Value::from_path(&path, "MOCK DATA".into())
                        }
                        FieldOrObjectType::Field(FieldType::Int) => {
                            Value::from_path(&path, 42usize.into())
                        }
                        FieldOrObjectType::Field(FieldType::Float) => {
                            Value::from_path(&path, 42.0f32.into())
                        }
                        FieldOrObjectType::Field(FieldType::Id) => {
                            Value::from_path(&path, "ID-MOCK-DATA".into())
                        }
                        FieldOrObjectType::Field(FieldType::Named(type_name)) => {
                            let child_type =
                                schema.object_types.get(type_name.as_str()).clone().expect(
                                    "this cannot happen on an already validated query; qed",
                                );
                            let mut sub_value = Default::default();
                            for (selection_set, field_type) in
                                child_type.fields.iter().filter_map(|(name, field_type)| {
                                    let selection_set = parent_selection_set
                                        .iter()
                                        .filter(|selection| match selection {
                                            Selection::Field {
                                                name: field_name, ..
                                            } => field_name == name,
                                            Selection::FragmentSpread { name: spread_name } => {
                                                spread_name == name
                                            }
                                            _ => false,
                                        })
                                        .cloned()
                                        .collect::<Vec<_>>();
                                    if selection_set.is_empty() {
                                        None
                                    } else {
                                        Some((selection_set, field_type))
                                    }
                                })
                            {
                                self.do_stuff(
                                    selection_set.as_slice(),
                                    &mut sub_value,
                                    schema,
                                    &FieldOrObjectType::Field(field_type),
                                    path.clone(),
                                );
                            }
                            sub_value
                        }
                        FieldOrObjectType::Field(FieldType::NonNull(non_null_type)) => {
                            "MOCK NON NULL STUFF".into()
                        }
                        FieldOrObjectType::Object(object_type) => {
                            let field_type = object_type
                                .fields
                                .get(name.as_str())
                                .expect("this cannot happen on an already validated query; qed");

                            let mut sub_value = Default::default();
                            self.do_stuff(
                                selection_set
                                    .as_ref()
                                    .expect("object_types require selection sets; qed"),
                                &mut sub_value,
                                schema,
                                &FieldOrObjectType::Field(field_type),
                                parent_path.clone(),
                            );

                            Value::from_path(&path, sub_value)
                        }
                        _ => unreachable!("lists and objects have been dealt with already; qed"),
                    };
                    output.deep_merge(sub_value);
                }
                Selection::InlineFragment {
                    fragment:
                        Fragment {
                            type_condition,
                            selection_set,
                        },
                } => {
                    self.do_stuff(
                        selection_set,
                        output,
                        schema,
                        field_or_object_type,
                        parent_path.clone(),
                    );
                }
                Selection::FragmentSpread { name } => {
                    if let Some(fragment) = self
                        .fragments
                        .get(name)
                        .or_else(|| schema.fragments.get(name))
                    {
                        self.do_stuff(
                            &fragment.selection_set,
                            output,
                            schema,
                            &field_or_object_type,
                            parent_path.clone(),
                        );
                    } else {
                        panic!("Missing fragment named: {}", name);
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
enum FieldOrObjectType<'f> {
    Field(&'f FieldType),
    Object(&'f ObjectType),
}

#[derive(Debug)]
struct Operation {
    name: Option<String>,
    selection_set: Vec<Selection>,
    variables: HashMap<String, FieldType>,
}

impl From<ast::OperationDefinition> for Operation {
    // Spec: https://spec.graphql.org/draft/#sec-Language.Operations
    fn from(operation: ast::OperationDefinition) -> Self {
        let name = operation.name().map(|x| x.text().to_string());
        let selection_set = operation
            .selection_set()
            .expect("the node SelectionSet is not optional in the spec; qed")
            .selections()
            .map(Into::into)
            .collect();
        let variables = operation
            .variable_definitions()
            .iter()
            .flat_map(|x| x.variable_definitions())
            .map(|definition| {
                let name = definition
                    .variable()
                    .expect("the node Variable is not optional in the spec; qed")
                    .name()
                    .expect("the node Name is not optional in the spec; qed")
                    .text()
                    .to_string();
                let ty = FieldType::from(
                    definition
                        .ty()
                        .expect("the node Type is not optional in the spec; qed"),
                );

                (name, ty)
            })
            .collect();

        Operation {
            selection_set,
            name,
            variables,
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
enum OperationType {
    Query,
    Mutation,
    Subscription,
}

impl From<ast::OperationType> for OperationType {
    // Spec: https://spec.graphql.org/draft/#OperationType
    fn from(operation_type: ast::OperationType) -> Self {
        if operation_type.query_token().is_some() {
            Self::Query
        } else if operation_type.mutation_token().is_some() {
            Self::Mutation
        } else if operation_type.subscription_token().is_some() {
            Self::Subscription
        } else {
            unreachable!(
                "either the `query` token is provided, either the `mutation` token, \
                either the `subscription` token; qed"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json_bytes::json;
    use test_log::test;

    macro_rules! assert_eq_and_ordered {
        ($a:expr, $b:expr $(,)?) => {
            assert_eq!($a, $b,);
            assert!(
                $a.eq_and_ordered(&$b),
                "assertion failed: objects are not ordered the same:\
                \n  left: `{:?}`\n right: `{:?}`",
                $a,
                $b,
            );
        };
    }

    macro_rules! assert_format_response {
        ($schema:expr, $query:expr, $response:expr, $operation:expr, $expected:expr $(,)?) => {{
            let schema: Schema = $schema.parse().expect("could not parse schema");
            let query = Query::parse($query).expect("could not parse query");
            let mut response = Response::builder().data($response.clone()).build();
            query.format_response(&mut response, $operation, &schema);
            assert_eq_and_ordered!(response.data, $expected);
        }};
    }

    #[test]
    fn reformat_response_data_field() {
        assert_format_response!(
            "",
            "{
                foo
                stuff{bar}
                array{bar}
                baz
                alias:baz
                alias_obj:baz_obj{bar}
                alias_array:baz_array{bar}
            }",
            json! {{
                "foo": "1",
                "stuff": {"bar": "2"},
                "array": [{"bar": "3", "baz": "4"}, {"bar": "5", "baz": "6"}],
                "baz": "7",
                "alias": "7",
                "alias_obj": {"bar": "8"},
                "alias_array": [{"bar": "9", "baz": "10"}, {"bar": "11", "baz": "12"}],
                "other": "13",
            }},
            None,
            json! {{
                "foo": "1",
                "stuff": {
                    "bar": "2",
                },
                "array": [
                    {"bar": "3"},
                    {"bar": "5"},
                ],
                "baz": "7",
                "alias": "7",
                "alias_obj": {
                    "bar": "8",
                },
                "alias_array": [
                    {"bar": "9"},
                    {"bar": "11"},
                ],
            }},
        );
    }

    #[test]
    fn reformat_response_data_inline_fragment() {
        assert_format_response!(
            "",
            "{... on Stuff { stuff{bar}} ... on Thing { id }}",
            json! {
                {"__typename": "Stuff", "id": "1", "stuff": {"bar": "2"}}
            },
            None,
            json! {{
                "stuff": {
                    "bar": "2",
                },
            }},
        );

        assert_format_response!(
            "",
            "{... on Stuff { stuff{bar}} ... on Thing { id }}",
            json! {
                {"__typename": "Thing", "id": "1", "stuff": {"bar": "2"}}
            },
            None,
            json! {{
                "id": "1",

            }},
        );
    }

    #[test]
    fn reformat_response_data_fragment_spread() {
        assert_format_response!(
            "fragment baz on Baz {baz}",
            "{...foo ...bar ...baz} fragment foo on Foo {foo} fragment bar on Bar {bar}",
            json! {
            {"__typename": "Foo", "foo": "1", "bar": "2", "baz": "3"}
            },
            None,
            json! {
                {"foo": "1"}
            },
        );
        assert_format_response!(
            "fragment baz on Baz {baz}",
            "{...foo ...bar ...baz} fragment foo on Foo {foo} fragment bar on Bar {bar}",
            json! {
            {"__typename": "Bar", "foo": "1", "bar": "2", "baz": "3"}
            },
            None,
            json! {
                {"bar": "2"}
            },
        );
        assert_format_response!(
            "fragment baz on Baz {baz}",
            "{...foo ...bar ...baz} fragment foo on Foo {foo} fragment bar on Bar {bar}",
            json! {
            {"__typename": "Baz", "foo": "1", "bar": "2", "baz": "3"}
            },
            None,
            json! {
                {"baz": "3"}
            },
        );
    }

    #[test]
    fn reformat_response_data_best_effort() {
        assert_format_response!(
            "",
            "{foo stuff{bar baz} ...fragment array{bar baz} other{bar}}",
            json! {{
                "foo": "1",
                "stuff": {"baz": "2"},
                "array": [
                    {"baz": "3"},
                    "4",
                    {"bar": "5"},
                ],
                "other": "6",
            }},
            None,
            json! {{
                "foo": "1",
                "stuff": {
                    "baz": "2",
                },
                "array": [
                    {"baz": "3"},
                    "4",
                    {"bar": "5"},
                ],
                "other": "6",
            }},
        );
    }

    #[test]
    fn reformat_matching_operation() {
        let schema = "";
        let query = "query MyOperation { foo }";
        let response = json! {{
            "foo": "1",
            "other": "2",
        }};
        assert_format_response!(
            schema,
            query,
            response,
            Some("OtherOperation"),
            json! {{
                "foo": "1",
                "other": "2",
            }},
        );
        assert_format_response!(
            schema,
            query,
            response,
            Some("MyOperation"),
            json! {{
                "foo": "1",
            }},
        );
    }

    macro_rules! run_validation {
        ($schema:expr, $query:expr, $variables:expr $(,)?) => {{
            let variables = match $variables {
                Value::Object(object) => object,
                _ => unreachable!("variables must be an object"),
            };
            let schema: Schema = $schema.parse().expect("could not parse schema");
            let request = Request::builder()
                .variables(variables)
                .query($query)
                .build();
            let query = Query::parse(
                request
                    .query
                    .as_ref()
                    .expect("query has been added right above; qed"),
            )
            .expect("could not parse query");
            query.validate_variables(&request, &schema)
        }};
    }

    macro_rules! assert_validation {
        ($schema:expr, $query:expr, $variables:expr $(,)?) => {{
            let res = run_validation!($schema, $query, $variables);
            assert!(res.is_ok(), "validation should have succeeded: {:?}", res);
        }};
    }

    macro_rules! assert_validation_error {
        ($schema:expr, $query:expr, $variables:expr $(,)?) => {{
            let res = run_validation!($schema, $query, $variables);
            assert!(res.is_err(), "validation should have failed");
        }};
    }

    #[test]
    fn variable_validation() {
        assert_validation!("", "query($foo:Boolean){x}", json!({}));
        assert_validation_error!("", "query($foo:Boolean!){x}", json!({}));
        assert_validation!("", "query($foo:Boolean!){x}", json!({"foo":true}));
        assert_validation!("", "query($foo:Boolean!){x}", json!({"foo":"true"}));
        assert_validation_error!("", "query($foo:Boolean!){x}", json!({"foo":"str"}));
        assert_validation!("", "query($foo:Int){x}", json!({}));
        assert_validation!("", "query($foo:Int){x}", json!({"foo":2}));
        assert_validation_error!("", "query($foo:Int){x}", json!({"foo":2.0}));
        assert_validation_error!("", "query($foo:Int){x}", json!({"foo":"str"}));
        assert_validation!("", "query($foo:Int){x}", json!({"foo":"2"}));
        assert_validation_error!("", "query($foo:Int){x}", json!({"foo":true}));
        assert_validation_error!("", "query($foo:Int){x}", json!({"foo":{}}));
        assert_validation_error!(
            "",
            "query($foo:Int){x}",
            json!({ "foo": i32::MAX as i64 + 1 })
        );
        assert_validation_error!(
            "",
            "query($foo:Int){x}",
            json!({ "foo": i32::MIN as i64 - 1 })
        );
        assert_validation!("", "query($foo:Int){x}", json!({ "foo": i32::MAX }));
        assert_validation!("", "query($foo:Int){x}", json!({ "foo": i32::MIN }));
        assert_validation!("", "query($foo:ID){x}", json!({"foo": "1"}));
        assert_validation!("", "query($foo:ID){x}", json!({"foo": 1}));
        assert_validation_error!("", "query($foo:ID){x}", json!({"foo": true}));
        assert_validation_error!("", "query($foo:ID){x}", json!({"foo": {}}));
        assert_validation!("", "query($foo:String){x}", json!({"foo": "str"}));
        assert_validation!("", "query($foo:Float){x}", json!({"foo":2.0}));
        assert_validation!("", "query($foo:Float){x}", json!({"foo":"2.0"}));
        assert_validation_error!("", "query($foo:Float){x}", json!({"foo":2}));
        assert_validation_error!("", "query($foo:Int!){x}", json!({}));
        assert_validation!("", "query($foo:[Int]){x}", json!({}));
        assert_validation_error!("", "query($foo:[Int]){x}", json!({"foo":1}));
        assert_validation_error!("", "query($foo:[Int]){x}", json!({"foo":"str"}));
        assert_validation_error!("", "query($foo:[Int]){x}", json!({"foo":{}}));
        assert_validation_error!("", "query($foo:[Int]!){x}", json!({}));
        assert_validation!("", "query($foo:[Int]!){x}", json!({"foo":[]}));
        assert_validation!("", "query($foo:[Int]){x}", json!({"foo":[1,2,3]}));
        assert_validation_error!("", "query($foo:[Int]){x}", json!({"foo":["f","o","o"]}));
        assert_validation!("", "query($foo:[Int]){x}", json!({"foo":["1","2","3"]}));
        assert_validation!("", "query($foo:[String]){x}", json!({"foo":["1","2","3"]}));
        assert_validation_error!("", "query($foo:[String]){x}", json!({"foo":[1,2,3]}));
        assert_validation!("", "query($foo:[Int!]){x}", json!({"foo":[1,2,3]}));
        assert_validation_error!("", "query($foo:[Int!]){x}", json!({"foo":[1,null,3]}));
        assert_validation!("", "query($foo:[Int]){x}", json!({"foo":[1,null,3]}));
        assert_validation!("type Foo{}", "query($foo:Foo){x}", json!({}));
        assert_validation!("type Foo{}", "query($foo:Foo){x}", json!({"foo":{}}));
        assert_validation_error!("type Foo{}", "query($foo:Foo){x}", json!({"foo":1}));
        assert_validation_error!("type Foo{}", "query($foo:Foo){x}", json!({"foo":"str"}));
        assert_validation_error!("type Foo{x:Int!}", "query($foo:Foo){x}", json!({"foo":{}}));
        assert_validation!(
            "type Foo{x:Int!}",
            "query($foo:Foo){x}",
            json!({"foo":{"x":1}})
        );
        assert_validation!(
            "type Foo implements Bar interface Bar{x:Int!}",
            "query($foo:Foo){x}",
            json!({"foo":{"x":1}}),
        );
        assert_validation_error!(
            "type Foo implements Bar interface Bar{x:Int!}",
            "query($foo:Foo){x}",
            json!({"foo":{"x":"str"}}),
        );
        assert_validation_error!(
            "type Foo implements Bar interface Bar{x:Int!}",
            "query($foo:Foo){x}",
            json!({"foo":{}}),
        );
        assert_validation!("scalar Foo", "query($foo:Foo!){x}", json!({"foo":{}}));
        assert_validation!("scalar Foo", "query($foo:Foo!){x}", json!({"foo":1}));
        assert_validation_error!("scalar Foo", "query($foo:Foo!){x}", json!({}));
        assert_validation!(
            "type Foo{bar:Bar!} type Bar{x:Int!}",
            "query($foo:Foo){x}",
            json!({"foo":{"bar":{"x":1}}})
        );
    }
}
