use color_eyre::Result;
use polars::prelude::DataFrame;

use crate::domain::feature_group;

use super::entities::FeatureGroup;

impl FeatureGroup {
    // pub async fn get_or_create_feature_group(
    //     &self,
    //     name: String,
    //     version: i32,
    //     description: Option<String>,
    //     features: Vec<Feature>,
    //     primary_key: Vec<String>,
    //     event_time: Option<String>,
    // ) -> Result<Self> {
    //     let fetched = feature_group::controller::get_feature_group_by_name_and_version(
    //         self.featurestore_id,
    //         name.as_str(),
    //         version,
    //     )
    //     .await?;

    //     match fetched {
    //         None => {
    //             let new_feature_group_payload =
    //                 feature_group::controller::make_new_feature_group_payload(
    //                     name.as_str(),
    //                     version,
    //                     description.as_deref(),
    //                     features
    //                         .iter()
    //                         .map(|f| {
    //                             feature::controller::make_new_feature_payload(
    //                                 f.name.as_str(),
    //                                 f.data_type.as_str(),
    //                                 f.description.as_deref(),
    //                             )
    //                         })
    //                         .collect(),
    //                     primary_key.iter().map(|pk| pk.as_str()).collect(),
    //                     event_time.as_deref(),
    //                 );
    //             Ok(feature_group::controller::create_feature_group(
    //                 self.featurestore_id,
    //                 new_feature_group_payload,
    //             )
    //             .await?
    //             .into())
    //         }
    //         Some(fg) => Ok(fg.into()),
    //     }
    // }
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
