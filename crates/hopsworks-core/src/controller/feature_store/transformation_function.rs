use color_eyre::Result;
use crate::feature_store::feature_view::transformation_function::TransformationFunction;
use hopsworks_internal::feature_store::transformation_function;

pub async fn get_transformation_function_by_name_and_version(
    feature_store_id: i32,
    name: &str,
    version: Option<i32>,
) -> Result<Option<TransformationFunction>> {
    if let Some(dto) =
        transformation_function::service::get_transformation_function_by_name_and_version(
            feature_store_id,
            name,
            version,
        )
        .await?
    {
        return Ok(Some(TransformationFunction::from(dto)));
    }

    Ok(None)
}
