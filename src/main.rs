use std::env;

pub mod client;

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
        panic!("You need to use a combination of email and password or an API key to authenticate.")
    }

    let project_id: i32 = 119;
    let feature_store_id: i32 = 67;

    let feature_group_list = the_client
        .get(
            format!("project/{project_id}/featurestores/{feature_store_id}/featuregroups").as_str(),
        )
        .await?;

    println!("{:?}", feature_group_list.status());
    println!("{:?}", feature_group_list.text_with_charset("utf-8").await?);

    Ok(())
}
