use super::{from_names_and_values, CompatRequest};
use crate::Error;
use crate::{Context, Object, Path};
use http::{Response, StatusCode};
use serde_json_bytes::Value;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Default, Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option)))]
pub struct SubgraphResponse {
    pub label: Option<String>,
    #[builder(setter(!strip_option))]
    pub data: Value,
    pub path: Option<Path>,
    pub has_next: Option<bool>,
    #[builder(setter(!strip_option))]
    pub errors: Vec<Error>,
    #[builder(default, setter(!strip_option, transform = |extensions: Vec<(&str, Value)>| Some(from_names_and_values(extensions))))]
    extensions: Option<Object>,
    context: Option<Context<CompatRequest>>,
}

impl SubgraphResponse {
    pub fn with_status(self, status: StatusCode) -> crate::SubgraphResponse {
        crate::SubgraphResponse {
            response: Response::builder()
                .status(status)
                .body(
                    crate::Response {
                        label: self.label,
                        data: self.data,
                        path: self.path,
                        has_next: self.has_next,
                        errors: self.errors,
                        extensions: self.extensions.unwrap_or_default(),
                    }
                    .into(),
                )
                .expect("crate::Response implements Serialize; qed")
                .into(),
            context: self
                .context
                .unwrap_or_else(|| Context::new().with_request(Arc::new(Default::default()))),
        }
    }
}

impl From<SubgraphResponse> for crate::SubgraphResponse {
    fn from(response: SubgraphResponse) -> Self {
        response.with_status(StatusCode::OK)
    }
}
