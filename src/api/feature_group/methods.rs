use color_eyre::Result;

use crate::domain::{feature, feature_group};

use super::entities::{Feature, FeatureGroup};

impl FeatureGroup {
    pub async fn get_or_create_feature_group(
        &self,
        name: String,
        version: i32,
        description: Option<String>,
        features: Vec<Feature>,
        primary_key: Vec<String>,
        event_time: Option<String>,
    ) -> Result<Self> {
        let fetched = feature_group::controller::get_feature_group_by_name_and_version(
            self.featurestore_id,
            name.as_str(),
            version,
        )
        .await?;

        match fetched {
            None => {
                let new_feature_group_payload =
                    feature_group::controller::make_new_feature_group_payload(
                        name.as_str(),
                        version,
                        description.as_deref(),
                        features
                            .iter()
                            .map(|f| {
                                feature::controller::make_new_feature_payload(
                                    f.name.as_str(),
                                    f.data_type.as_str(),
                                    f.description.as_deref(),
                                )
                            })
                            .collect(),
                        primary_key.iter().map(|pk| pk.as_str()).collect(),
                        event_time.as_deref(),
                    );
                Ok(feature_group::controller::create_feature_group(
                    self.featurestore_id,
                    new_feature_group_payload,
                )
                .await?
                .into())
            }
            Some(fg) => Ok(fg.into()),
        }
    }
}
