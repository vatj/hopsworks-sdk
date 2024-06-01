use serde::{Deserialize, Serialize};

pub mod service;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowBaseParams {
    template_id: i32,
    flow_chunk_size: usize,
    pub(super) flow_total_size: usize,
    flow_identifier: String,
    pub(super) flow_filename: String,
    flow_relative_path: String,
    flow_total_chunks: usize,
    flow_current_chunk_size: usize,
    flow_chunk_number: usize,
}

impl FlowBaseParams {
    pub fn to_query_params(&self) -> Vec<(&str, String)> {
        vec![
            ("flowChunkNumber", self.flow_chunk_number.to_string()),
            ("flowChunkSize", self.flow_chunk_size.to_string()),
            (
                "flowCurrentChunkSize",
                self.flow_current_chunk_size.to_string(),
            ),
            ("flowTotalSize", self.flow_total_size.to_string()),
            ("flowIdentifier", self.flow_identifier.clone()),
            ("flowFilename", self.flow_filename.clone()),
            ("flowRelativePath", self.flow_relative_path.clone()),
            ("flowTotalChunks", self.flow_total_chunks.to_string()),
        ]
    }
}