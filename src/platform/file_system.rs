use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadOptions {
    pub chunk_size: usize,
    pub simultaneous_uploads: usize,
    pub max_chunk_retries: i32,
}

impl UploadOptions {
    pub fn new(chunk_size: usize, simultaneous_uploads: usize, max_chunk_retries: i32) -> Self {
        UploadOptions {
            chunk_size,
            simultaneous_uploads,
            max_chunk_retries,
        }
    }
}

impl Default for UploadOptions {
    fn default() -> Self {
        UploadOptions {
            chunk_size: 1048576,
            simultaneous_uploads: 3,
            max_chunk_retries: 3,
        }
    }
}
