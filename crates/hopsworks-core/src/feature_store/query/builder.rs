use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

#[derive(Debug, Serialize, Deserialize, Clone, TypedBuilder)]
pub struct BatchQueryOptions {
    #[builder(setter(strip_option))]
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    #[builder(setter(strip_option))]
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
    #[builder(default = false)]
    pub event_time: bool,
    #[builder(default = false)]
    pub primary_keys: bool,
    #[builder(default = false)]
    pub inference_helper_columns: bool,
    #[builder(setter(strip_option))]
    pub td_version: Option<i32>,
    #[builder(default = false)]
    pub label: bool,
    #[builder(default = false, setter(skip))]
    pub transformed: bool,
}
