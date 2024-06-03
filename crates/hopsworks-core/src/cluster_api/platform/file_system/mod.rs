use serde::{Deserialize, Serialize};

pub mod service;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowBaseParams {
    template_id: i32,
    flow_chunk_size: usize,
    pub flow_total_size: usize,
    flow_identifier: String,
    pub flow_filename: String,
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

    pub fn new(chunk_size: usize, num_chunks: usize, total_size: usize, file_name: &str) -> Self {
        Self {
            template_id: -1,
            flow_chunk_size: chunk_size,
            flow_total_size: total_size,
            flow_identifier: format!("{}_{}", total_size, file_name),
            flow_filename: file_name.to_string(),
            flow_relative_path: file_name.to_string(),
            flow_total_chunks: num_chunks,
            flow_current_chunk_size: 0,
            flow_chunk_number: 0,
        }
    }

    pub fn get_chunk_params(&self, chunk_size: usize, chunk_number: usize) -> FlowBaseParams {
        let mut params = self.clone();
        params.flow_current_chunk_size = chunk_size;
        params.flow_chunk_number = chunk_number;
        params
    }
}
