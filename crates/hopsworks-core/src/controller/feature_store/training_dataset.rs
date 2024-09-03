use color_eyre::Result;
use tracing::debug;

use crate::{
    cluster_api::feature_store::{
        statistics_config::StatisticsConfigDTO,
        training_dataset::{payloads::TrainingDatasetSplitPayload, TrainingDatasetType},
    },
    controller::feature_store::query::construct_query,
    feature_store::{
        feature_view::{
            training_dataset::TrainingDataset,
            training_dataset_builder::{TrainingDatasetBuilder, TrainingDatasetBuilderState},
            training_dataset_typed_builder::{SizeSplit, TrainingDatasetMetadata},
            FeatureView,
        },
        query::Query,
    },
};

use crate::cluster_api::{
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
        crate::controller::feature_store::feature_view::features_to_transformed_features(
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

pub fn build_training_dataset_payload(
    metadata: &TrainingDatasetMetadata,
    size_split: &SizeSplit,
) -> NewTrainingDatasetPayloadV2 {
    let training_dataset_type = if metadata.storage_connector.is_some() {
        match metadata.location {
            Some(_) => TrainingDatasetType::HopsFS,
            None => TrainingDatasetType::External,
        }
    } else {
        TrainingDatasetType::InMemory
    };

    let mut splits = Vec::with_capacity(3);
    splits.push(TrainingDatasetSplitPayload::new_with_size(
        "train".to_string(),
        size_split.train,
    ));
    if size_split.validation.is_some() {
        splits.push(TrainingDatasetSplitPayload::new_with_size(
            "validation".to_string(),
            size_split.validation.unwrap(),
        ));
    }
    if size_split.test.is_some() {
        splits.push(TrainingDatasetSplitPayload::new_with_size(
            "test".to_string(),
            size_split.test.unwrap(),
        ));
    }

    NewTrainingDatasetPayloadV2 {
        dto_type: "trainingDatasetDTO".to_string(),
        featurestore_id: metadata.feature_store_id,
        name: metadata.feature_view_name.clone(),
        version: None,
        event_start_time: None,
        event_end_time: None,
        coalesce: metadata.coalesce,
        seed: metadata.seed,
        data_format: metadata.data_format.clone(),
        description: metadata.description.clone(),
        location: metadata.location.clone(),
        training_dataset_type,
        statistics_config: metadata
            .statistics_config
            .as_ref()
            .map(StatisticsConfigDTO::from),
        storage_connector: metadata.storage_connector.clone(),
        train_split: Some("train".to_string()),
        splits,
    }
}
