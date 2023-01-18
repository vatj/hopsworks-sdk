use std::collections::HashMap;

use color_eyre::Result;
use reqwest::StatusCode;

use crate::get_hopsworks_client;

use super::entities::{TransformationFunctionDTO, TransformationFunctionResponse};

pub async fn get_transformation_function_by_name_and_version(
    feature_store_id: i32,
    name: &str,
    version: Option<i32>,
) -> Result<Option<TransformationFunctionDTO>> {
    let mut query_params = HashMap::new();
    query_params.insert("name", name.to_owned());
    if let Some(ver) = version {
        query_params.insert("version", ver.to_string());
    }
    // let query_params = [("name", name.to_owned())];

    let res = get_hopsworks_client()
        .await
        .get_with_project_id_and_auth(
            format!("featurestores/{}/transformationfunctions", feature_store_id).as_str(),
            true,
            true,
        )
        .await?
        .query(&query_params)
        .send()
        .await?;

    let transformation_function_response= match res.status() {
        StatusCode::OK => res.json::<TransformationFunctionResponse>().await?,
        _ => panic!("Transformation function get request failed with status {:?}, here is the response : \n{:?}.",
            res.status(),
            res.text_with_charset("utf-8").await)
    };

    if transformation_function_response.items.is_empty() {
        Ok(None)
    } else {
        Ok(transformation_function_response.items.first().cloned())
    }
}
