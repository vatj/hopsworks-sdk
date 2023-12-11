pub struct UploadOptions {
    pub chunk_size: usize,
    pub simultaneous_uploads: usize,
    pub overwrite: bool,
}
