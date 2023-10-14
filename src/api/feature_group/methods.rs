use color_eyre::Result;
use polars::prelude::DataFrame;

use crate::{api::query::entities::Query, domain::feature_group, util};

use super::entities::{Feature, FeatureGroup, StatisticsConfig, User};

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

    pub async fn read_with_arrow_flight_client(&self) -> Result<()> {
        let query_object =
            self.select(self.get_feature_names().iter().map(|s| s as &str).collect())?;
        let read_df =
            feature_group::controller::read_feature_group_with_arrow_flight_client(query_object)
                .await?;

        Ok(())
    }
}
