use color_eyre::Result;

use crate::repositories::platform::file_system::service;

pub async fn file_or_dir_exists(path: &str) -> Result<bool> {
    let resp = service::get_path_metadata(path).await;

    match resp {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

pub async fn remove_file_or_dir(path: &str) -> Result<()> {
    service::remove(path).await
}

pub async fn move_file_or_dir(src_path: &str, dst_path: &str, overwrite: bool) -> Result<()> {
    let dst_exists = file_or_dir_exists(dst_path).await?;

    if overwrite && dst_exists {
        remove_file_or_dir(dst_path).await?;
    } else if !overwrite && dst_exists {
        return Err(color_eyre::eyre::eyre!(
            "Destination path {} already exists, set overwrite=Some(true) to overwrite it",
            dst_path,
        ));
    }

    service::move_file_or_dir(src_path, dst_path).await?;

    Ok(())
}

pub async fn copy(src_path: &str, dst_path: &str, overwrite: bool) -> Result<()> {
    let dst_exists = file_or_dir_exists(dst_path).await?;

    if overwrite && dst_exists {
        remove_file_or_dir(dst_path).await?;
    } else if !overwrite && dst_exists {
        return Err(color_eyre::eyre::eyre!(
            "Destination path {} already exists, set overwrite=Some(true) to overwrite it",
            dst_path,
        ));
    }

    service::copy(src_path, dst_path).await?;

    Ok(())
}
