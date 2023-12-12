use std::clone::Clone;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{self};
use std::path::Path;
use std::thread;
use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};

pub mod service;

pub struct Chunk {
    content: String,
    number: i32,
    status: String,
    retries: i32,
}

impl Chunk {
    pub fn new(content: String, number: i32, status: String) -> Self {
        Chunk {
            content,
            number,
            status,
            retries: 0,
        }
    }
}

pub const DEFAULT_FLOW_CHUNK_SIZE: usize = 1048576;
pub const FLOW_PERMANENT_ERRORS: [u16; 5] = [404, 413, 415, 500, 501];

pub fn upload(
    &self,
    local_path: &str,
    upload_path: &str,
    overwrite: bool,
    chunk_size: usize,
    simultaneous_uploads: usize,
    max_chunk_retries: i32,
    chunk_retry_interval: u64,
) -> Result<String, RestAPIError> {
    // local path could be absolute or relative,
    let local_path = if !Path::new(local_path).is_absolute() && Path::new(local_path).exists() {
        let cwd = std::env::current_dir().unwrap();
        cwd.join(local_path)
    } else {
        Path::new(local_path).to_owned()
    };

    let file_size = fs::metadata(&local_path)?.len() as usize;
    let file_name = local_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let destination_path = format!("{}/{}", upload_path, file_name);

    if self.exists(&destination_path)? {
        if overwrite {
            self.remove(&destination_path)?;
        } else {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "{} already exists, set overwrite=True to overwrite it",
                    destination_path
                ),
            )
            .into());
        }
    }

    let num_chunks = (file_size as f64 / chunk_size as f64).ceil() as usize;

    let base_params = self._get_flow_base_params(&file_name, num_chunks, file_size, chunk_size);

    let mut chunk_number = 1;
    let file = File::open(&local_path)?;
    let pbar = ProgressBar::new(file_size as u64);
    pbar.set_style(
            ProgressStyle::default_bar()
                .template("{desc}: {percentage:.3}%|{bar}| {bytes}/{total_bytes} elapsed<{elapsed} remaining<{eta}")?
                .progress_chars("#>-"),
        );

    let chunks: Vec<Chunk> = file
        .bytes()
        .enumerate()
        .map(|(index, byte)| {
            let chunk_content = byte?;
            Ok(Chunk::new(
                chunk_content,
                index as i32 + 1,
                "pending".to_string(),
            ))
        })
        .collect::<Result<Vec<Chunk>, io::Error>>()?;

    let mut handles = vec![];
    for chunk in chunks {
        let base_params = base_params.clone();
        let upload_path = upload_path.to_owned();
        let file_name = file_name.to_owned();
        let pbar = pbar.clone();
        let max_chunk_retries = max_chunk_retries.clone();
        let chunk_retry_interval = chunk_retry_interval.clone();

        let handle = thread::spawn(move || {
            self._upload_chunk(
                base_params,
                &upload_path,
                &file_name,
                chunk,
                pbar,
                max_chunk_retries,
                chunk_retry_interval,
            )
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap()?;
    }

    pbar.finish();
    Ok(format!("{}/{}", upload_path, file_name))
}

fn _upload_chunk(
    &self,
    base_params: HashMap<&str, String>,
    upload_path: &str,
    file_name: &str,
    chunk: Chunk,
    pbar: ProgressBar,
    max_chunk_retries: i32,
    chunk_retry_interval: u64,
) -> Result<(), RestAPIError> {
    let mut query_params = base_params.clone();
    query_params.insert("flowCurrentChunkSize", chunk.content.len().to_string());
    query_params.insert("flowChunkNumber", chunk.number.to_string());

    chunk.status = "uploading".to_string();
    loop {
        match self._upload_request(&query_params, upload_path, file_name, &chunk.content) {
            Ok(_) => break,
            Err(re) => {
                chunk.retries += 1;
                if DatasetApi::FLOW_PERMANENT_ERRORS.contains(&re.response.status_code)
                    || chunk.retries > max_chunk_retries
                {
                    chunk.status = "failed".to_string();
                    return Err(re);
                }
                thread::sleep(Duration::from_secs(chunk_retry_interval));
                continue;
            }
        }
    }

    chunk.status = "uploaded".to_string();

    pbar.inc(chunk.content.len() as u64);
    Ok(())
}

fn _upload_request(
    params: &HashMap<&str, String>,
    path: &str,
    file_name: &str,
    chunk: &str,
) -> Result<(), RestAPIError> {
    let _client = client::get_instance();
    let path_params = vec!["project", &self.project_id, "dataset", "upload", path];

    let mut form = reqwest::multipart::Form::new();
    for (key, value) in params {
        form = form.text(key.to_string(), value.to_string());
    }
    form = form.file("file", file_name)?;

    _client._send_request("POST", &path_params, None, true, Some(form))?;
    Ok(())
}
