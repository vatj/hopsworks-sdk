use color_eyre::Result;
use polars::prelude::DataFrame;

use crate::domain::feature_group;

use super::entities::FeatureGroup;

impl FeatureGroup {
    fn set_id(&self, id: i32) {
        self.id.set(Some(id));
    }

    pub fn get_id(&self) -> Option<i32> {
        self.id.get()
    }

    fn set_online_topic_name(&self, online_topic_name: Option<String>) {
        *self.online_topic_name.borrow_mut() = online_topic_name;
    }

    pub fn get_online_topic_name(&self) -> Option<String> {
        self.online_topic_name.borrow().clone()
    }

    pub fn get_primary_keys(&self) -> Result<Vec<&str>> {
        let primary_keys = self
            .features
            .iter()
            .filter(|f| f.primary)
            .map(|f| f.name.as_str())
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
                )
                .unwrap(),
            )
            .await?;

            self.set_id(feature_group_dto.id);
            self.set_online_topic_name(feature_group_dto.online_topic_name);
        }

        feature_group::controller::insert_in_registered_feature_group(
            self.name.as_str(),
            self.version,
            self.get_online_topic_name().unwrap_or_default().as_str(),
            dataframe,
            self.get_primary_keys().unwrap(),
        )
        .await
    }
}
