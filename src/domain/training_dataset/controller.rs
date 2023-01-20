use color_eyre::Result;

use crate::{
    api::{
        feature_view::entities::FeatureView, query::entities::Query,
        training_dataset::entities::TrainingDataset,
    },
    domain::query::controller::construct_query,
    repositories::{
        feature::entities::{FeatureDTO, TrainingDatasetFeatureDTO},
        feature_group::entities::FeatureGroupDTO,
        feature_view::service as feature_view_service,
        job::entities::JobDTO,
        query::entities::QueryDTO,
        training_datasets::payloads::{
            NewTrainingDatasetPayload, TrainingDatasetComputeJobConfigPayload,
        },
        transformation_function::entities::TransformationFunctionDTO,
    },
};

pub async fn create_train_test_split() -> Result<()> {
    todo!("create_train_test_spli is not implemented");
}

pub async fn create_training_dataset_attached_to_feature_view(
    feature_view: FeatureView,
) -> Result<TrainingDataset> {
    let features = feature_view
        .query
        .left_features
        .clone()
        .iter()
        .map(|feature| {
            TrainingDatasetFeatureDTO::new_from_feature_and_transformation_function(
                FeatureDTO::from(feature.clone()),
                FeatureGroupDTO::from(feature_view.query.left_feature_group.clone()),
                match feature_view.transformation_functions.get(&feature.name) {
                    Some(transformation_function) => Some(TransformationFunctionDTO::from(
                        transformation_function.clone(),
                    )),
                    None => None,
                },
            )
        })
        .collect();

    let new_training_dataset_payload = NewTrainingDatasetPayload::new(
        feature_view.feature_store_id,
        feature_view.feature_store_name.clone(),
        "trans_view_1_1".to_owned(),
        6,
        QueryDTO::from(feature_view.query.clone()),
        Some(construct_query(feature_view.query.clone()).await?),
        features,
    );

    let training_dataset_dto =
        feature_view_service::create_training_dataset_attached_to_feature_view(
            &feature_view.name,
            feature_view.version,
            new_training_dataset_payload,
        )
        .await?;

    println!("The training dataset :\n{:?}", training_dataset_dto);

    let job_dto = compute_training_dataset_attached_to_feature_view(
        feature_view.feature_store_id,
        &feature_view.name,
        feature_view.version,
        training_dataset_dto.version,
        feature_view.query.clone(),
    )
    .await?;

    println!("The job :\n{:?}", job_dto);

    Ok(TrainingDataset {})
}

pub async fn compute_training_dataset_attached_to_feature_view(
    feature_store_id: i32,
    feature_view_name: &str,
    feature_view_version: i32,
    training_dataset_version: i32,
    query: Query,
) -> Result<JobDTO> {
    let job_config = TrainingDatasetComputeJobConfigPayload::new(true, QueryDTO::from(query));

    Ok(
        feature_view_service::compute_training_dataset_attached_to_feature_view(
            feature_store_id,
            feature_view_name,
            feature_view_version,
            training_dataset_version,
            job_config,
        )
        .await?,
    )
}
