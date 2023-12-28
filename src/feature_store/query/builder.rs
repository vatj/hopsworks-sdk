use std::default;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BatchQueryOptions {
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub end_time: chrono::DateTime<chrono::Utc>,
    pub td_version: Option<i32>,
    pub with_primary_keys: bool,
    pub with_event_time: bool,
    pub with_label: bool,
    pub inference_helper_columns: Vec<String>,
    pub training_helper_columns: Vec<String>,
}

impl BatchQueryOptions {
    pub fn new(
        start_time: chrono::DateTime<chrono::Utc>,
        end_time: chrono::DateTime<chrono::Utc>,
        td_version: Option<i32>,
        with_primary_keys: bool,
        with_event_time: bool,
        with_label: bool,
        inference_helper_columns: Vec<String>,
        training_helper_columns: Vec<String>,
    ) -> Self {
        Self {
            start_time,
            end_time,
            td_version,
            with_primary_keys,
            with_event_time,
            with_label,
            inference_helper_columns,
            training_helper_columns,
        }
    }

    pub fn with_start_time(mut self, start_time: chrono::DateTime<chrono::Utc>) -> Self {
        self.start_time = start_time;
        self
    }

    pub fn with_end_time(mut self, end_time: chrono::DateTime<chrono::Utc>) -> Self {
        self.end_time = end_time;
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

impl default::Default for BatchQueryOptions {
    fn default() -> Self {
        Self {
            start_time: chrono::Utc::now()
                .checked_sub_signed(chrono::Duration::days(1))
                .unwrap(),
            end_time: chrono::Utc::now(),
            td_version: None,
            with_primary_keys: false,
            with_event_time: false,
            with_label: false,
            inference_helper_columns: vec![],
            training_helper_columns: vec![],
        }
    }
}
