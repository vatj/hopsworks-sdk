use std::collections::HashMap;

use color_eyre::Result;

use crate::cluster_api::feature_store::{
    feature::{FeatureDTO, TrainingDatasetFeatureDTO},
    feature_group::FeatureGroupDTO,
    feature_view::{self, payloads::NewFeatureViewPayload},
    query::QueryDTO,
    transformation_function::TransformationFunctionDTO,
};
use crate::{
    controller::feature_store::query::construct_query,
    feature_store::FeatureGroup,
    feature_store::{
        feature_group::feature::Feature,
        feature_view::{transformation_function::TransformationFunction, FeatureView},
        query::{builder::BatchQueryOptions, Query},
    },
};

pub async fn create_feature_view(
    feature_store_id: i32,
    feature_store_name: &str,
    name: &str,
    version: i32,
    query: &Query,
    transformation_functions: Option<HashMap<String, TransformationFunction>>,
    description: Option<&str>,
) -> Result<FeatureView> {
    let transformation_functions = transformation_functions.unwrap_or_default();
    let (features, feature_groups) = query.features_and_feature_groups();
    let training_features =
        features_to_transformed_features(&features, &feature_groups, &transformation_functions)?;

    let query_string = construct_query(query).await?;
    Ok(FeatureView::from(
        feature_view::service::create_feature_view(
            feature_store_id,
            NewFeatureViewPayload::new(
                feature_store_id,
                feature_store_name,
                name,
                version,
                QueryDTO::from(query),
                Some(&query_string),
                training_features,
                description,
            ),
        )
        .await?,
    ))
}

pub async fn get_feature_view_by_name_and_version(
    feature_store_id: i32,
    name: &str,
    version: Option<i32>,
) -> Result<Option<FeatureView>> {
    match feature_view::service::get_feature_view_by_name_and_version(
        feature_store_id,
        name,
        version,
    )
    .await?
    {
        Some(feature_view_dto) => Ok(Some(FeatureView::from(feature_view_dto))),
        None => Ok(None),
    }
}

pub async fn get_batch_query_string(
    feature_view: &FeatureView,
    batch_query_options: &BatchQueryOptions,
) -> Result<String> {
    let batch_query = get_batch_query(feature_view, batch_query_options).await?;
    let fs_query = construct_query(&batch_query).await?;

    Ok(fs_query.pit_query.unwrap_or(fs_query.query))
}

pub async fn get_batch_query(
    feature_view: &FeatureView,
    batch_query_options: &BatchQueryOptions,
) -> Result<Query> {
    let batch_query_payload =
        feature_view::payloads::FeatureViewBatchQueryPayload::from(batch_query_options);
    let query_dto = feature_view::service::get_feature_view_batch_query(
        feature_view.feature_store_id(),
        feature_view.name(),
        feature_view.version(),
        batch_query_payload,
    )
    .await?;

    Ok(Query::from(query_dto))
}

pub fn features_to_transformed_features(
    features: &[&Feature],
    feature_groups: &[&FeatureGroup],
    transformation_functions: &HashMap<String, TransformationFunction>,
) -> Result<Vec<TrainingDatasetFeatureDTO>> {
    Ok(features
        .iter()
        .zip(feature_groups.iter())
        .map(|(feature, feature_group)| {
            TrainingDatasetFeatureDTO::new_from_feature_and_transformation_function(
                &FeatureDTO::from(*feature),
                &FeatureGroupDTO::from(*feature_group),
                transformation_functions
                    .get(feature.name())
                    .map(|transformation_function| {
                        TransformationFunctionDTO::from(transformation_function)
                    }),
            )
        })
        .collect())
}

pub async fn delete(feature_view: &FeatureView) -> Result<()> {
    feature_view::service::delete_feature_view_by_name_and_version(
        feature_view.feature_store_id(),
        feature_view.name(),
        feature_view.version(),
    )
    .await
}
