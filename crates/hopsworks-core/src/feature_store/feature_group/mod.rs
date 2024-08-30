//! Feature Group API
//!
//! Feature Groups are a central abstractions in Hopsworks. They represent a logical grouping of features
//! and serve as the primary interface through which one can ingest Feature data to the Feature Store.
//! To learn more about Feature Groups, see the [documentation](https://docs.hopsworks.ai/latest/concepts/fs/feature_group/fg_overview/).
//!
//! Feature groups are central to Feature Engineering pipelines. A common use case is to schedule a job that
//! pulls data from an external data source, performs some transformations on it,
//! and then inserts the data via the Feature Group.
pub mod feature;
pub mod statistics_config;

use color_eyre::Result;
use tracing::debug;
use serde::{Deserialize, Serialize};

use crate::feature_store::{query::Query, FeatureStore};
use crate::util;
use crate::cluster_api::feature_store::feature_group::FeatureGroupDTO;

use self::{feature::Feature, statistics_config::StatisticsConfig};

use crate::platform::user::User;

#[cfg(feature = "polars")]
use polars::prelude::DataFrame;
#[cfg(feature = "polars")]
use crate::controller::feature_store::feature_group;

use super::embedding::embedding_index::EmbeddingIndex;

/// Feature Group are metadata objects describing a table in the Feature Store.
/// They are the primary interface through which one can ingest Feature data to the Feature Store.
/// Once a Feature Group is created, one can insert/upsert data to it using the `insert` method.
///
/// Feature Group entities do not by itself encapsulate any of the Feature data in the table.
/// Rather it provides methods to insert/upsert additional data from the corresponding table in the Feature Store.
///
/// # Note
/// Feature Groups also implements some methods to read the available data from the Feature Store.
/// However, they are mainly intended for monitoring purposes. We strongly recommend using the
/// `Feature View` object to read data from the Feature Store for any data science or machine learning use case.
///
/// # Examples
/// ```no_run
/// use color_eyre::Result;
///
/// use polars::prelude::*;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///   let feature_store = hopsworks::login(None).await?.get_feature_store().await?;
///
///   let mut feature_group = feature_store
///      .get_feature_group("my_feature_group", Some(1))
///      .await?
///      .expect("Feature Group not found");
///
///   let mut mini_df = df! [
///     "number" => [2i64, 3i64],
///     "word" => ["charlie", "dylan"]
///   ]?;
///
///   feature_group.insert(&mut mini_df).await?;
///
///   Ok(())
///  }
/// ```
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FeatureGroup {
    id: Option<i32>,
    featurestore_id: i32,
    featurestore_name: String,
    feature_group_type: String,
    description: Option<String>,
    created: String,
    creator: Option<User>,
    version: i32,
    name: String,
    location: Option<String>,
    statistics_config: Option<StatisticsConfig>,
    features: Vec<Feature>,
    online_enabled: bool,
    time_travel_format: String,
    online_topic_name: Option<String>,
    primary_key: Option<Vec<String>>,
    event_time: Option<String>,
    embedding_index: Option<EmbeddingIndex>
}

impl FeatureGroup {
    pub fn new_local(
        feature_store: &FeatureStore,
        name: &str,
        version: i32,
        description: Option<&str>,
        primary_key: Vec<&str>,
        event_time: Option<&str>,
        online_enabled: bool,
    ) -> Self {
        Self {
            featurestore_id: feature_store.featurestore_id,
            featurestore_name: feature_store.featurestore_name.clone(),
            feature_group_type: String::from("STREAM_FEATURE_GROUP"),
            description: description.map(String::from),
            created: String::from(""),
            creator: None,
            version,
            name: String::from(name),
            id: None,
            location: None,
            statistics_config: None,
            features: Vec::new(),
            online_enabled,
            time_travel_format: String::from("NONE"),
            online_topic_name: None,
            primary_key: Some(primary_key.iter().map(|pk| pk.to_string()).collect()),
            event_time: event_time.map(String::from),
            embedding_index: None
        }
    }
}

impl From<FeatureGroupDTO> for FeatureGroup {
    fn from(feature_group_dto: FeatureGroupDTO) -> Self {
        FeatureGroup {
            featurestore_id: feature_group_dto.featurestore_id,
            featurestore_name: feature_group_dto.featurestore_name,
            feature_group_type: feature_group_dto.feature_group_type,
            description: feature_group_dto.description,
            created: feature_group_dto.created,
            creator: Some(User::new_from_dto(feature_group_dto.creator)),
            version: feature_group_dto.version,
            name: feature_group_dto.name,
            id: Some(feature_group_dto.id),
            location: Some(feature_group_dto.location),
            statistics_config: feature_group_dto
                .statistics_config
                .as_ref()
                .map(StatisticsConfig::new_from_dto),
            features: feature_group_dto
                .features
                .iter()
                .map(|feature_dto| Feature::new_from_dto(feature_dto.to_owned()))
                .collect(),
            online_enabled: feature_group_dto.online_enabled,
            time_travel_format: feature_group_dto.time_travel_format,
            online_topic_name: feature_group_dto.online_topic_name,
            primary_key: Some(feature_group_dto.features.iter().filter_map(|f| {
                if f.primary {
                    Some(f.name.clone())
                } else {
                    None
                }
            }).collect()),
            event_time: feature_group_dto.event_time,
            embedding_index: feature_group_dto.embedding_index.map(EmbeddingIndex::from)
        }
    }
}

impl FeatureGroup {
    pub fn id(&self) -> Option<i32> {
        self.id
    }

    pub fn get_project_name(&self) -> String {
        util::strip_feature_store_suffix(&self.featurestore_name)
    }

    pub fn feature_store_id(&self) -> i32 {
        self.featurestore_id
    }

    pub fn feature_store_name(&self) -> &str {
        self.featurestore_name.as_str()
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn created(&self) -> &str {
        self.created.as_str()
    }

    pub(crate) fn feature_group_type(&self) -> &str {
        self.feature_group_type.as_str()
    }

    pub fn is_online_enabled(&self) -> bool {
        self.online_enabled
    }

    pub(crate) fn time_travel_format(&self) -> &str {
        self.time_travel_format.as_str()
    }

    pub fn is_time_travel_enabled(&self) -> bool {
        self.time_travel_format == "HUDI"
    }

    pub fn online_topic_name(&self) -> Option<&str> {
        self.online_topic_name.as_deref()
    }

    pub fn creator(&self) -> Option<&User> {
        self.creator.as_ref()
    }

    pub fn location(&self) -> Option<&str> {
        self.location.as_deref()
    }

    pub fn event_time(&self) -> Option<&str> {
        self.event_time.as_deref()
    }

    pub fn statistics_config(&self) -> Option<&StatisticsConfig> {
        self.statistics_config.as_ref()
    }

    pub fn features(&self) -> &Vec<Feature> {
        &self.features
    }

    pub fn features_mut(&mut self) -> &mut Vec<Feature> {
        &mut self.features
    }

    /// Returns the list of primary keys for the feature group.
    ///
    /// Note that order matters when building primary keys to access values from the online Feature Store.
    pub fn primary_keys(&self) -> Vec<&str> {
        debug!(
            "Getting primary keys for feature group {}: {:?}",
            self.name(),
            self.primary_key
        );
        self
            .primary_key
            .as_ref()
            .unwrap_or_else(|| panic!("Primary key not set for feature group {}", self.name()))
            .iter()
            .map(|pk| pk.as_str())
            .collect()
    }

    pub fn primary_keys_owned(&self) -> Vec<String> {
        self
            .primary_key
            .as_ref()
            .unwrap_or_else(|| panic!("Primary key not set for feature group {}", self.name()))
            .iter()
            .map(|pk| pk.to_owned())
            .collect()
    }

    pub fn embedding_index(&self) -> Option<&EmbeddingIndex> {
        self.embedding_index.as_ref()
    }

    pub fn embedding_index_mut(&mut self) -> &mut Option<EmbeddingIndex> {
        &mut self.embedding_index
    }

    /// Returns the feature with the given name if exists.
    ///
    /// # Arguments
    /// * `feature_name` - The name of the feature to get.
    pub fn feature_by_name(&self, feature_name: &str) -> Option<&Feature> {
        self.features()
            .iter()
            .find(|feature| feature.name() == feature_name)
    }

    /// Returns the list of owned feature names for the feature group.
    pub fn feature_names(&self) -> Vec<&str> {
        self.features.iter().map(|f| f.name()).collect()
    }

    /// Returns the list of owned feature names for the feature group.
    pub fn feature_names_owned(&self) -> Vec<String> {
        self.features()
            .iter()
            .map(|feature| feature.name().to_string())
            .collect()
    }

    /// Selects a subset of features from the feature group and returns a query object.
    /// The query object can be used to read data from the feature group.
    /// # Arguments
    /// * `feature_names` - A slice of feature names to select from the feature group.
    ///
    /// # Example
    /// ```no_run
    /// use color_eyre::Result;
    ///
    /// use polars::prelude::*;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///  let project = hopsworks::login(None).await?;
    ///  let feature_store = project.get_feature_store().await?;
    ///
    ///  let feature_group = feature_store
    ///    .get_feature_group("my_feature_group", Some(1))
    ///    .await?
    ///    .expect("Feature Group not found");
    ///
    ///  let query = feature_group.select(&["number", "word"])?;
    ///
    ///  let df = query.read_from_offline_feature_store(None).await?;
    ///
    ///  Ok(())
    /// }
    /// ```
    pub fn select(&self, feature_names: &[&str]) -> Result<Query> {
        debug!(
            "Selecting features {:?} from feature group {} to build query object",
            feature_names, self.name
        );
        Ok(Query::new_no_joins_no_filter(
            self.clone(),
            self.features()
                .iter()
                .filter_map(|feature| {
                    if feature_names.contains(&feature.name()) {
                        Some(feature.clone())
                    } else {
                        None
                    }
                })
                .collect(),
        ))
    }

    pub fn select_all(&self) -> Query {
        debug!(
            "Selecting all features from feature group {} to build query object",
            self.name
        );
        Query::new_no_joins_no_filter(self.clone(), self.features().to_vec())
    }

    #[cfg(feature = "polars")]
    pub async fn register_feature_group(&mut self, dataframe: &DataFrame) -> Result<()> {
        if self.id().is_none() {
        let feature_group_dto = feature_group::save_feature_group_metadata(
            self.featurestore_id,
            feature_group::build_new_feature_group_payload(
                self.name(),
                self.version(),
                self.description(),
                self.primary_key
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|pk| pk.as_ref())
                    .collect(),
                self.event_time.as_deref(),
                dataframe.schema(),
                self.online_enabled,
            )
            .unwrap(),
        )
        .await?;

        self.id = Some(feature_group_dto.id);
        self.online_topic_name = feature_group_dto.online_topic_name;
        self.creator = Some(User::from(feature_group_dto.creator));
        self.location = Some(feature_group_dto.location);
        self.statistics_config = feature_group_dto
            .statistics_config
            .as_ref()
            .map(StatisticsConfig::from);
        self.features_mut()
            .extend(feature_group_dto.features.into_iter().map(Feature::from));

         Ok(())
    } else {
        Err(color_eyre::eyre::eyre!("Feature Group already registered."))
    }
    }

    pub async fn delete(&self) -> Result<()> {
        if self.id().is_none() {
            Err(color_eyre::eyre::eyre!("Feature Group not registered."))
        } else {
            feature_group::delete_feature_group(self.feature_store_id(), self.id().unwrap()).await
        }
        
    }
    
}

