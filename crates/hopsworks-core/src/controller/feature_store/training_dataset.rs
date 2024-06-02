use color_eyre::Result;
use log::debug;
use polars::frame::DataFrame;

use crate::{
    controller::feature_store::query::construct_query,
    feature_store::feature_view::training_dataset_builder::{
        TrainingDatasetBuilder, TrainingDatasetBuilderState,
    },
    {
        feature_store::feature_view::{training_dataset::TrainingDataset, FeatureView},
        feature_store::query::Query,
    },
};

use hopsworks_internal::{
        feature_store::{
            feature_view::service as feature_view_service,
            query::QueryDTO,
            training_dataset::{
                self,
                payloads::{
                    NewTrainingDatasetPayload, NewTrainingDatasetPayloadV2,
                    TrainingDatasetComputeJobConfigPayload,
                },
            },
        },
        platform::job::JobDTO,
    };

pub async fn create_train_test_split() -> Result<()> {
    todo!("create_train_test_split is not implemented");
}

pub async fn register_training_dataset<S>(
    builder: &TrainingDatasetBuilder<S>,
) -> Result<TrainingDataset>
where
    S: TrainingDatasetBuilderState,
{
    let payload = NewTrainingDatasetPayloadV2::from(builder);

    Ok(TrainingDataset::from(
        &training_dataset::service::create_feature_view_training_dataset(
            builder.feature_store_id,
            builder.feature_view_name.as_str(),
            builder.feature_view_version,
            payload,
        )
        .await?,
    ))
}

pub async fn read_from_offline_feature_store(
    _training_dataset: &TrainingDataset,
) -> Result<DataFrame> {
    todo!("read_from_offline_feature_store is not implemented");
}

pub async fn materialize_on_cluster<S>(
    _training_dataset_builder: &TrainingDatasetBuilder<S>,
) -> Result<TrainingDataset>
where
    S: TrainingDatasetBuilderState,
{
    todo!("materialize_on_cluster is not implemented");
}

pub async fn create_training_dataset_attached_to_feature_view(
    feature_view: &FeatureView,
) -> Result<TrainingDataset> {
    let (features, feature_groups) = feature_view.query().features_and_feature_groups();
    let training_features =
        crate::core::feature_store::feature_view::features_to_transformed_features(
            &features,
            &feature_groups,
            feature_view.transformation_functions(),
        )?;

    let new_training_dataset_payload = NewTrainingDatasetPayload::new(
        feature_view.feature_store_id(),
        feature_view.feature_store_name().to_string(),
        "trans_view_1_1".to_owned(),
        1,
        QueryDTO::from(feature_view.query()),
        Some(construct_query(feature_view.query()).await?),
        training_features,
    );

    let training_dataset_dto =
        feature_view_service::create_training_dataset_attached_to_feature_view(
            feature_view.name(),
            feature_view.version(),
            new_training_dataset_payload,
        )
        .await?;

    debug!("The training dataset :\n{:#?}", training_dataset_dto);

    let job_dto = compute_training_dataset_attached_to_feature_view(
        feature_view.feature_store_id(),
        feature_view.name(),
        feature_view.version(),
        training_dataset_dto.version,
        feature_view.query(),
    )
    .await?;

    debug!("The job :\n{:?}", job_dto);

    Ok(TrainingDataset::new(
        &training_dataset_dto.featurestore_name,
        training_dataset_dto.version,
    ))
}

pub async fn compute_training_dataset_attached_to_feature_view(
    feature_store_id: i32,
    feature_view_name: &str,
    feature_view_version: i32,
    training_dataset_version: i32,
    query: &Query,
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
        Some(training_dataset_dto) => Ok(Some(TrainingDataset::new(
            &training_dataset_dto.featurestore_name,
            training_dataset_dto.version,
        ))),
        None => Ok(None),
    }
}
