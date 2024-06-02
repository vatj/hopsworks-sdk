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
use log::debug;
use serde::{Deserialize, Serialize};

use crate::feature_store::{query::Query, FeatureStore};

#[cfg(feature = "polars_insert")]
use platform::job_execution::JobExecution;
#[cfg(feature = "polars_insert")]
use crate::controller::feature_store::feature_group;
#[cfg(feature = "polars_insert")]
use polars::frame::DataFrame;

#[cfg(feature = "read_arrow_flight_offline_store")]
use hopsworks_offline_store::read_with_arrow_flight_client;
#[cfg(feature = "read_arrow_flight_offline_store")]
use super::query::read_option::OfflineReadOptions;
#[cfg(feature = "read_arrow_flight_offline_store")]
use crate::controller::feature_store::feature_group;

use hopsworks_internal::{feature_store::{feature_group::FeatureGroupDTO, statistics_config::StatisticsConfigDTO, feature::FeatureDTO}, platform::users::UserDTO, util};

use self::{feature::Feature, statistics_config::StatisticsConfig};

use crate::platform::user::User;

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
}

impl FeatureGroup {
    pub fn new_from_dto(feature_group_dto: FeatureGroupDTO) -> Self {
        Self {
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
            primary_key: None,
            event_time: None,
        }
    }

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
        }
    }
}

impl From<FeatureGroupDTO> for FeatureGroup {
    fn from(feature_group_dto: FeatureGroupDTO) -> Self {
        FeatureGroup::new_from_dto(feature_group_dto)
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

    pub(crate) fn feature_store_name(&self) -> &str {
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

    pub(crate) fn is_online_enabled(&self) -> bool {
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
    pub fn get_primary_keys(&self) -> Result<Vec<&str>> {
        Ok(self
            .primary_key
            .as_ref()
            .unwrap_or_else(|| panic!("Primary key not set for feature group {}", self.name()))
            .iter()
            .map(|pk| pk.as_str())
            .collect())
    }

    pub fn get_primary_keys_owned(&self) -> Result<Vec<String>> {
        Ok(self
            .primary_key
            .as_ref()
            .unwrap_or_else(|| panic!("Primary key not set for feature group {}", self.name()))
            .iter()
            .map(|pk| pk.to_owned())
            .collect())
    }

    /// Returns the feature with the given name if exists.
    ///
    /// # Arguments
    /// * `feature_name` - The name of the feature to get.
    pub fn get_feature(&self, feature_name: &str) -> Option<&Feature> {
        self.features()
            .iter()
            .find(|feature| feature.name() == feature_name)
    }

    /// Inserts or upserts data into the Feature Group table.
    ///
    /// Dataframe is written row by row to the project Kafka topic.
    /// A Hudi job is then triggered to materialize the data into the offline Feature Group table.
    ///
    /// If the Feature Group is online enabled, Hopsworks onlineFS service
    /// writes rows by primary key to RonDB. Only the most recent value for a primary key
    /// is stored.
    ///
    /// # Arguments
    /// * `dataframe` - A mutable reference to a Polars DataFrame containing the data to insert.
    ///
    /// # Returs
    /// A JobExecution object containing information about status of the insertion job.
    ///
    /// # Example
    /// ```no_run
    /// use color_eyre::Result;
    ///
    /// use polars::prelude::*;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   let project = hopsworks::login(None).await?;
    ///   let feature_store = project.get_feature_store().await?;
    ///
    ///   let mut feature_group = feature_store
    ///     .get_feature_group("my_feature_group", Some(1))
    ///     .await?
    ///     .expect("Feature Group not found");
    ///
    ///   let mut mini_df = df! [
    ///     "number" => [2i64, 3i64],
    ///     "word" => ["charlie", "dylan"]
    ///   ]?;
    ///
    ///  feature_group.insert(&mut mini_df).await?;
    ///
    ///  Ok(())
    /// }
    /// ```
    #[cfg(feature = "polars_insert")]
    pub async fn insert(&mut self, dataframe: &mut DataFrame) -> Result<JobExecution> {
        if self.id().is_none() {
            let feature_group_dto = feature_group::save_feature_group_metadata(
                self.featurestore_id,
                feature_group::build_new_feature_group_payload(
                    &self.name,
                    self.version,
                    self.description.as_deref(),
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
        }

        feature_group::insert_in_registered_feature_group(
            self.featurestore_id,
            self.id().unwrap(),
            self.name.as_str(),
            self.version,
            self.online_topic_name().unwrap_or_default(),
            dataframe,
            &self.get_primary_keys()?,
        )
        .await
    }

    /// Returns the list of owned feature names for the feature group.
    pub fn get_feature_names(&self) -> Vec<&str> {
        self.features.iter().map(|f| f.name()).collect()
    }

    /// Returns the list of owned feature names for the feature group.
    pub fn get_feature_names_owned(&self) -> Vec<String> {
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
            "Selecting features {:?} from feature group {}, building query object",
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

    /// Reads feature group data from Hopsworks via the Arrow Flight client.
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
    ///    .get_feature_group("my_feature_group", None)
    ///    .await?
    ///    .expect("Feature Group not found");
    ///
    ///  let df = feature_group.read_from_offline_feature_store(None).await?;
    ///
    ///  Ok(())
    /// }
    /// ```
    #[cfg(feature = "read_arrow_flight_offline_store")]
    pub async fn read_from_offline_feature_store(
        &self,
        offline_read_options: Option<OfflineReadOptions>,
    ) -> Result<DataFrame> {
        let query = self.select(&self.get_feature_names())?;
        debug!(
            "Reading data from feature group {} with Arrow Flight client",
            self.name
        );
        let read_df = read_with_arrow_flight_client(query, offline_read_options).await?;

        Ok(read_df)
    }
}

impl From<&FeatureGroup> for FeatureGroupDTO {
    fn from(feature_group: &FeatureGroup) -> Self {
        FeatureGroupDTO {
            id: feature_group.id().unwrap_or(0),
            online_topic_name: feature_group
                .online_topic_name()
                .map(|online_topic_name| online_topic_name.to_string()),
            creator: match feature_group.creator() {
                Some(user) => UserDTO::from(user.clone()),
                None => panic!("creator field should not be None for an initialized FeatureGroup"),
            },
            location: feature_group.location().unwrap_or("").to_string(),
            statistics_config: Some(match feature_group.statistics_config() {
                Some(statistics_config) => StatisticsConfigDTO::from(statistics_config),
                None => panic!(
                    "statistics_config field should not be None for an initialized FeatureGroup"
                ),
            }),
            features: feature_group
                .features()
                .iter()
                .map(FeatureDTO::from)
                .collect(),
            feature_group_type: match feature_group.feature_group_type() {
                "STREAM_FEATURE_GROUP" => "streamFeatureGroupDTO".to_owned(),
                "streamFeatureGroupDTO" => "streamFeatureGroupDTO".to_owned(),
                _ => "streamFeatureGroupDTO".to_owned(),
            },
            featurestore_id: feature_group.feature_store_id(),
            featurestore_name: feature_group.feature_store_name().to_string(),
            description: feature_group
                .description()
                .map(|description| description.to_string()),
            created: feature_group.created().to_string(),
            version: feature_group.version(),
            name: feature_group.name().to_string(),

            online_enabled: feature_group.is_online_enabled(),
            time_travel_format: feature_group.time_travel_format().to_string(),
        }
    }
}