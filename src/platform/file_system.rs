use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadOptions {
    pub chunk_size: usize,
    pub simultaneous_uploads: usize,
    pub max_chunk_retries: i32,
    pub chunk_retry_interval: u64,
}

impl UploadOptions {
    pub fn new(
        chunk_size: usize,
        simultaneous_uploads: usize,
        max_chunk_retries: i32,
        chunk_retry_interval: u64,
    ) -> Self {
        UploadOptions {
            chunk_size,
            simultaneous_uploads,
            max_chunk_retries,
            chunk_retry_interval,
        }
    }
}

impl Default for UploadOptions {
    fn default() -> Self {
        UploadOptions {
            chunk_size: 1048576,
            simultaneous_uploads: 3,
            max_chunk_retries: 3,
            chunk_retry_interval: 1000,
        }
    }
}
