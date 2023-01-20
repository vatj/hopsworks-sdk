use color_eyre::Result;

use crate::{
    api::{feature_view::entities::FeatureView, training_dataset::entities::TrainingDataset},
    domain::query::controller::construct_query,
    repositories::{
        feature::entities::{FeatureDTO, TrainingDatasetFeatureDTO},
        feature_group::entities::FeatureGroupDTO,
        feature_view::service::create_training_dataset_attached_to_feature_view,
        job::entities::JobDTO,
        query::entities::QueryDTO,
        training_datasets::payloads::NewTrainingDatasetPayload,
        transformation_function::entities::TransformationFunctionDTO,
    },
};

pub async fn create_train_test_split() -> Result<()> {
    todo!("create_train_test_spli is not implemented");
}

pub async fn create_training_dataset_from_feature_view(
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
        feature_view.feature_store_name,
        "trans_view_1_1".to_owned(),
        1,
        QueryDTO::from(feature_view.query.clone()),
        Some(construct_query(feature_view.query).await?),
        features,
    );

    let dto = create_training_dataset_attached_to_feature_view(
        &feature_view.name,
        feature_view.version,
        new_training_dataset_payload,
    )
    .await?;

    println!("{:?}", dto);

    Ok(TrainingDataset {})
}

pub async fn compute_training_dataset() -> Result<JobDTO> {
    todo!("compute training dataset not implemented");
}
