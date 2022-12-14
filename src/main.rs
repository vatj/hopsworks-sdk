use std::env;

pub mod client;
pub mod models;

use log::info;

use crate::models::feature_group::FeatureGroupDTO;
use crate::models::feature_store::FeatureStoreDTO;

#[tokio::main]
async fn main() -> Result<(), reqwest::Error> {
    env_logger::init();

    let the_client: client::HopsworksClient = client::HopsworksClient::default();

    let email = env::var("HOPSWORKS_EMAIL").unwrap_or_default();
    let password = env::var("HOPSWORKS_PASSWORD").unwrap_or_default();
    let api_key = env::var("HOPSWORKS_API_KEY").unwrap_or_default();

    if email.len() > 1 && password.len() > 1 {
        the_client
            .login_with_email_and_password(&email, &password)
            .await?;
    } else if api_key.len() > 1 {
        the_client.set_api_key(api_key).await?;
    } else {
        panic!("Use a combination of email and password or an API key to authenticate.")
    }

    let project_id: i32 = 119;
    let feature_store_id: i32 = 67;
    let feature_group_id: i32 = 13;

    let feature_store: FeatureStoreDTO = the_client
        .get(format!("project/{project_id}/featurestores/{feature_store_id}").as_str())
        .await?
        .json()
        .await?;

    info!("{}", serde_json::to_string_pretty(&feature_store).unwrap());

    let feature_group : FeatureGroupDTO = the_client
        .get(format!("project/{project_id}/featurestores/{feature_store_id}/featuregroups/{feature_group_id}").as_str())
        .await?
        .json()
        .await?;

    info!("{}", serde_json::to_string_pretty(&feature_group).unwrap());

    Ok(())
}
