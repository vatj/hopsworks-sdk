use std::collections::HashMap;

use color_eyre::Result;

use crate::{
    core::feature_store::query::construct_query,
    feature_store::{
        feature_view::{transformation_function::TransformationFunction, FeatureView},
        query::entities::Query,
    },
    repositories::feature_store::{
        feature::entities::{FeatureDTO, TrainingDatasetFeatureDTO},
        feature_group::entities::FeatureGroupDTO,
        feature_view::{self, payloads::NewFeatureViewPayload},
        query::entities::QueryDTO,
        transformation_function::{self, entities::TransformationFunctionDTO},
    },
};

pub async fn create_feature_view(
    feature_store_id: i32,
    feature_store_name: String,
    name: String,
    version: i32,
    query: Query,
    transformation_functions: Option<HashMap<String, TransformationFunction>>,
) -> Result<FeatureView> {
    let transformation_functions = match transformation_functions {
        None => HashMap::<String, TransformationFunction>::new(),
        Some(transformation_functions) => transformation_functions,
    };
    let features = query
        .left_features
        .clone()
        .iter()
        .map(|feature| {
            TrainingDatasetFeatureDTO::new_from_feature_and_transformation_function(
                FeatureDTO::from(feature.clone()),
                FeatureGroupDTO::from(query.left_feature_group.clone()),
                transformation_functions
                    .get(&feature.name)
                    .map(|transformation_function| {
                        TransformationFunctionDTO::from(transformation_function.clone())
                    }),
            )
        })
        .collect();

    let query_string = construct_query(query.clone()).await?;
    Ok(FeatureView::from(
        feature_view::service::create_feature_view(
            feature_store_id,
            NewFeatureViewPayload::new(
                feature_store_id,
                feature_store_name,
                name,
                version,
                QueryDTO::from(query),
                Some(query_string),
                features,
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
