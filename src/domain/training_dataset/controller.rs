use color_eyre::Result;
use log::debug;

use crate::{
    api::{
        feature_view::{training_dataset::TrainingDataset, FeatureView},
        query::entities::Query,
    },
    domain::query::controller::construct_query,
    repositories::{
        feature::entities::{FeatureDTO, TrainingDatasetFeatureDTO},
        feature_group::entities::FeatureGroupDTO,
        feature_view::service as feature_view_service,
        job::entities::JobDTO,
        query::entities::QueryDTO,
        training_dataset::{
            self,
            payloads::{NewTrainingDatasetPayload, TrainingDatasetComputeJobConfigPayload},
        },
        transformation_function::entities::TransformationFunctionDTO,
    },
};

pub async fn create_train_test_split() -> Result<()> {
    todo!("create_train_test_split is not implemented");
}

pub async fn create_training_dataset_attached_to_feature_view(
    feature_view: &FeatureView,
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
                feature_view
                    .transformation_functions
                    .get(&feature.name)
                    .map(|transformation_function| {
                        TransformationFunctionDTO::from(transformation_function.clone())
                    }),
            )
        })
        .collect();

    let new_training_dataset_payload = NewTrainingDatasetPayload::new(
        feature_view.feature_store_id,
        feature_view.feature_store_name.clone(),
        "trans_view_1_1".to_owned(),
        1,
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

    debug!("The training dataset :\n{:#?}", training_dataset_dto);

    let job_dto = compute_training_dataset_attached_to_feature_view(
        feature_view.feature_store_id,
        &feature_view.name,
        feature_view.version,
        training_dataset_dto.version,
        feature_view.query.clone(),
    )
    .await?;

    debug!("The job :\n{:?}", job_dto);

    Ok(TrainingDataset {
        feature_store_name: training_dataset_dto.featurestore_name,
        version: training_dataset_dto.version,
    })
}

pub async fn compute_training_dataset_attached_to_feature_view(
    feature_store_id: i32,
    feature_view_name: &str,
    feature_view_version: i32,
    training_dataset_version: i32,
    query: Query,
) -> Result<JobDTO> {
    let job_config = TrainingDatasetComputeJobConfigPayload::new(true, QueryDTO::from(query));

    feature_view_service::compute_training_dataset_attached_to_feature_view(
        feature_store_id,
        feature_view_name,
        feature_view_version,
        training_dataset_version,
        job_config,
    )
    .await
}

pub async fn get_training_dataset_by_name_and_version(
    feature_store_id: i32,
    name: &str,
    version: Option<i32>,
) -> Result<Option<TrainingDataset>> {
    let opt_training_dataset_dto =
        training_dataset::service::get_training_dataset_by_name_and_version(
            feature_store_id,
            name,
            version,
        )
        .await?;

    match opt_training_dataset_dto {
        Some(training_dataset_dto) => Ok(Some(TrainingDataset {
            feature_store_name: training_dataset_dto.featurestore_name,
            version: training_dataset_dto.version,
        })),
        None => Ok(None),
    }
}
