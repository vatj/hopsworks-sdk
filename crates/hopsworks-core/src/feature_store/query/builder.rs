use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

#[derive(Debug, Serialize, Deserialize, Clone, TypedBuilder)]
pub struct BatchQueryOptions {
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    #[builder(default = false, setter(prefix = "with_"))]
    pub event_time: bool,
    #[builder(default = false, setter(prefix = "with_"))]
    pub primary_keys: bool,
    #[builder(default = false, setter(prefix = "with_"))]
    pub inference_helper_columns: bool,
    pub td_version: Option<i32>,
    #[builder(default = false, setter(prefix = "with_"))]
    pub label: bool,
    #[builder(default = false, setter(skip))]
    pub transformed: bool,
}
