//! Feature group API module
//!
//! Feature Groups are a central abstractions in Hopsworks. They represent a logical grouping of features
//! and serve as the primary interface through which one can ingest Feature data to the Feature Store.
//! To learn more about Feature Groups, see the [documentation](https://docs.hopsworks.ai/latest/concepts/fs/feature_group/fg_overview/).
//!
//! Feature groups are central to Feature Engineering pipelines. A common use case is to schedule a job that
//! pulls data from an external data source (see ava), performs some transformations on it,
//! and then inserts the data via the Feature Group.
pub mod feature;
pub mod statistics_config;

use color_eyre::Result;
use log::debug;
use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};

use crate::{
    api::{feature_store::entities::FeatureStore, query::entities::Query},
    domain::feature_group,
    repositories::feature_group::entities::FeatureGroupDTO,
    util,
};

use self::{feature::Feature, statistics_config::StatisticsConfig};

use super::platform::user::User;

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
/// use hopsworks_rs::hopsworks_login;
/// use polars::prelude::*;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///   let project = hopsworks_login(None).await?;
///   let feature_store = project.get_feature_store().await?;
///
///   let feature_group = feature_store
///      .get_feature_group_by_name_and_version("my_feature_group", 1)
///      .await?;
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
    pub(super) id: Cell<Option<i32>>,
    pub featurestore_id: i32,
    pub featurestore_name: String,
    pub feature_group_type: String,
    pub description: Option<String>,
    pub created: String,
    pub creator: RefCell<Option<User>>,
    pub version: i32,
    pub name: String,
    pub location: RefCell<Option<String>>,
    pub statistics_config: RefCell<Option<StatisticsConfig>>,
    pub features: RefCell<Vec<Feature>>,
    pub online_enabled: bool,
    pub time_travel_format: String,
    pub online_topic_name: RefCell<Option<String>>,
    pub primary_key: Option<Vec<String>>,
    pub event_time: Option<String>,
}

impl FeatureGroup {
    pub fn new_from_dto(feature_group_dto: FeatureGroupDTO) -> Self {
        Self {
            featurestore_id: feature_group_dto.featurestore_id,
            featurestore_name: feature_group_dto.featurestore_name,
            feature_group_type: feature_group_dto.feature_group_type,
            description: feature_group_dto.description,
            created: feature_group_dto.created,
            creator: RefCell::new(Some(User::new_from_dto(feature_group_dto.creator))),
            version: feature_group_dto.version,
            name: feature_group_dto.name,
            id: Cell::new(Some(feature_group_dto.id)),
            location: RefCell::new(Some(feature_group_dto.location)),
            statistics_config: RefCell::new(
                feature_group_dto
                    .statistics_config
                    .map(StatisticsConfig::new_from_dto),
            ),
            features: RefCell::new(
                feature_group_dto
                    .features
                    .iter()
                    .map(|feature_dto| Feature::new_from_dto(feature_dto.to_owned()))
                    .collect(),
            ),
            online_enabled: feature_group_dto.online_enabled,
            time_travel_format: feature_group_dto.time_travel_format,
            online_topic_name: RefCell::new(feature_group_dto.online_topic_name),
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
        event_time: &str,
        online_enabled: bool,
    ) -> Self {
        Self {
            featurestore_id: feature_store.featurestore_id,
            featurestore_name: feature_store.featurestore_name.clone(),
            feature_group_type: String::from("STREAM_FEATURE_GROUP"),
            description: description.map(String::from),
            created: String::from(""),
            creator: RefCell::new(None),
            version,
            name: String::from(name),
            id: Cell::new(None),
            location: RefCell::new(None),
            statistics_config: RefCell::new(None),
            features: RefCell::new(vec![]),
            online_enabled,
            time_travel_format: String::from("NONE"),
            online_topic_name: RefCell::new(None),
            primary_key: Some(primary_key.iter().map(|pk| pk.to_string()).collect()),
            event_time: Some(String::from(event_time)),
        }
    }
}

impl From<FeatureGroupDTO> for FeatureGroup {
    fn from(feature_group_dto: FeatureGroupDTO) -> Self {
        FeatureGroup::new_from_dto(feature_group_dto)
    }
}

impl FeatureGroup {
    fn set_id(&self, id: i32) {
        self.id.set(Some(id));
    }

    pub fn get_id(&self) -> Option<i32> {
        self.id.get()
    }

    pub fn get_project_name(&self) -> String {
        util::strip_feature_store_suffix(&self.featurestore_name)
    }

    fn set_online_topic_name(&self, online_topic_name: Option<String>) {
        *self.online_topic_name.borrow_mut() = online_topic_name;
    }

    pub fn get_online_topic_name(&self) -> Option<String> {
        self.online_topic_name.borrow().clone()
    }

    pub fn get_creator(&self) -> Option<User> {
        self.creator.borrow().clone()
    }

    fn set_creator(&self, creator: Option<User>) {
        *self.creator.borrow_mut() = creator;
    }

    pub fn get_location(&self) -> Option<String> {
        self.location.borrow().clone()
    }

    fn set_location(&self, location: Option<String>) {
        *self.location.borrow_mut() = location;
    }

    pub fn get_statistics_config(&self) -> Option<StatisticsConfig> {
        self.statistics_config.borrow().clone()
    }

    fn set_statisctics_config(&self, statistics_config: Option<StatisticsConfig>) {
        *self.statistics_config.borrow_mut() = statistics_config;
    }

    pub fn get_features(&self) -> Vec<Feature> {
        self.features.borrow().clone()
    }

    fn set_features(&self, features: Vec<Feature>) {
        *self.features.borrow_mut() = features;
    }

    pub fn get_primary_keys(&self) -> Result<Vec<String>> {
        let primary_keys = self
            .get_features()
            .iter()
            .filter(|f| f.primary)
            .map(|f| f.name.clone())
            .collect();

        Ok(primary_keys)
    }

    pub async fn insert(&self, dataframe: &mut DataFrame) -> Result<()> {
        if self.get_id().is_none() {
            let feature_group_dto = feature_group::controller::save_feature_group_metadata(
                self.featurestore_id,
                feature_group::controller::build_new_feature_group_payload(
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

            self.set_id(feature_group_dto.id);
            self.set_online_topic_name(feature_group_dto.online_topic_name);
            self.set_creator(Some(User::from(feature_group_dto.creator)));
            self.set_location(Some(feature_group_dto.location));
            // self.set_statisctics_config(match feature_group_dto.statistics_config {
            //     Some(config) => Some(StatisticsConfig::from(config)),
            //     None => None,
            // });
            self.set_statisctics_config(
                feature_group_dto
                    .statistics_config
                    .map(StatisticsConfig::from),
            );
            self.set_features(
                feature_group_dto
                    .features
                    .into_iter()
                    .map(Feature::from)
                    .collect(),
            );
        }

        feature_group::controller::insert_in_registered_feature_group(
            self.featurestore_id,
            self.get_id().unwrap(),
            self.name.as_str(),
            self.version,
            self.get_online_topic_name().unwrap_or_default().as_str(),
            dataframe,
            self.get_primary_keys().unwrap(),
        )
        .await
    }

    pub fn get_feature_names(&self) -> Vec<String> {
        self.get_features()
            .iter()
            .map(|feature| feature.name.clone())
            .collect()
    }

    pub fn select(&self, feature_names: Vec<&str>) -> Result<Query> {
        debug!(
            "Selecting features {:?} from feature group {}, building query object",
            feature_names, self.name
        );
        Ok(Query::new_no_joins_no_filter(
            self.clone(),
            self.get_features()
                .iter()
                .filter_map(|feature| {
                    if feature_names.contains(&feature.name.as_str()) {
                        Some(feature.clone())
                    } else {
                        None
                    }
                })
                .collect(),
        ))
    }

    pub async fn read_with_arrow_flight_client(&self) -> Result<DataFrame> {
        let query_object =
            self.select(self.get_feature_names().iter().map(|s| s as &str).collect())?;
        debug!(
            "Reading data from feature group {} with Arrow Flight client",
            self.name
        );
        let read_df =
            feature_group::controller::read_feature_group_with_arrow_flight_client(query_object)
                .await?;

        Ok(read_df)
    }
}
