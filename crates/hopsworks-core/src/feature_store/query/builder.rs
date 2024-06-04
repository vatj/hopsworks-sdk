use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct BatchQueryOptions {
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
    pub td_version: Option<i32>,
    pub with_primary_keys: bool,
    pub with_event_time: bool,
    pub with_label: bool,
    pub inference_helper_columns: Vec<String>,
    pub training_helper_columns: Vec<String>,
}

impl BatchQueryOptions {
    pub fn with_start_time(mut self, start_time: chrono::DateTime<chrono::Utc>) -> Self {
        self.start_time = Some(start_time);
        self
    }

    pub fn with_end_time(mut self, end_time: chrono::DateTime<chrono::Utc>) -> Self {
        self.end_time = Some(end_time);
        self
    }

    pub fn with_td_version(mut self, td_version: i32) -> Self {
        self.td_version = Some(td_version);
        self
    }

    pub fn with_primary_keys(mut self) -> Self {
        self.with_primary_keys = true;
        self
    }

    pub fn with_event_time(mut self) -> Self {
        self.with_event_time = true;
        self
    }

    pub fn with_label(mut self) -> Self {
        self.with_label = true;
        self
    }

    pub fn with_inference_helper_columns(mut self, inference_helper_columns: &[&str]) -> Self {
        self.inference_helper_columns = inference_helper_columns
            .iter()
            .map(|s| s.to_string())
            .collect();
        self
    }

    pub fn with_training_helper_columns(mut self, training_helper_columns: &[&str]) -> Self {
        self.training_helper_columns = training_helper_columns
            .iter()
            .map(|s| s.to_string())
            .collect();
        self
    }
}