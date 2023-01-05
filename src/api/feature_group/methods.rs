use color_eyre::Result;
use polars::prelude::DataFrame;

use crate::domain::feature_group;

use super::entities::FeatureGroup;

impl FeatureGroup {
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
        if self.id.is_none() {
            let _id = feature_group::controller::save_feature_group_metadata(
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

            todo!("Wrapping fg.id in mutex to update it without forcing fg to be declared mutable")
        }

        feature_group::controller::insert_in_registered_feature_group(
            self.id.unwrap_or(0),
            self.online_topic_name.clone().unwrap_or_default().as_str(),
            dataframe,
            self.get_primary_keys().unwrap(),
        )
        .await
    }
}
