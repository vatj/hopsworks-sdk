use color_eyre::Result;

use crate::cluster_api::platform::opensearch::service;

pub async fn get_opensearch_auth_token(project_id: i32) -> Result<String> {
    service::get_authorization_token(project_id).await
}