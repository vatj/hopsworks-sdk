//! Query API
pub mod filter;
pub mod join;

use color_eyre::Result;
use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};

pub use filter::{QueryFilter, QueryFilterOrLogic, QueryLogic};
pub use join::{JoinOptions, JoinQuery};

use crate::{
    core::feature_store::query::{
        read_query_from_online_feature_store, read_with_arrow_flight_client,
    },
    feature_store::feature_group::{feature::Feature, FeatureGroup},
    repositories::feature_store::query::entities::QueryDTO,
};

/// Query object are used to read data from the feature store, both online and offline.
///
/// They are usually constructed by calling `FeatureGroup.select()` and
/// joining with other queries using `Query.join()`.
/// You can subsequently use `Query.read_from_online_feature_store()`
/// or `Query.read_from_offline_feature_store()` to read your Feature data.
///
/// Query objects support:
/// - Joining with other queries
/// - filtering on individual features, see [Feature][`crate::feature_store::feature_group::Feature`]
/// - real-time reads from the online feature store if all features belong to online-enabled [Feature Group][`crate::feature_store::feature_group::FeatureGroup`]s
/// - offline (so-called batch) reads from the offline feature store if all features belong to offline-enabled [Feature Group][`crate::feature_store::feature_group::FeatureGroup`]s
/// - full or per-query time travel, via the `Query.as_of()` method, if all features belong to time travel enabled [Feature Group][`crate::feature_store::feature_group::FeatureGroup`]s
///
/// # Examples
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Query {
    left_feature_group: FeatureGroup,
    left_features: Vec<Feature>,
    feature_store_name: String,
    feature_store_id: i32,
    joins: Option<Vec<JoinQuery>>,
    filters: Option<Vec<QueryFilterOrLogic>>,
    left_feature_group_start_time: Option<String>,
    left_feature_group_end_time: Option<String>,
}

impl Query {
    pub fn new_no_joins_no_filter(
        left_feature_group: FeatureGroup,
        left_features: Vec<Feature>,
    ) -> Self {
        Self {
            feature_store_name: left_feature_group.feature_store_name().to_string(),
            feature_store_id: left_feature_group.feature_store_id(),
            left_feature_group,
            left_features,
            joins: Some(vec![]),
            filters: None,
            left_feature_group_start_time: None,
            left_feature_group_end_time: None,
        }
    }

    pub fn feature_store_name(&self) -> &str {
        self.feature_store_name.as_ref()
    }

    pub fn feature_store_id(&self) -> i32 {
        self.feature_store_id
    }

    pub fn left_feature_group(&self) -> &FeatureGroup {
        &self.left_feature_group
    }

    pub fn left_features(&self) -> &Vec<Feature> {
        &self.left_features
    }

    pub fn left_feature_group_start_time(&self) -> Option<&str> {
        self.left_feature_group_start_time.as_deref()
    }

    pub fn left_feature_group_end_time(&self) -> Option<&str> {
        self.left_feature_group_end_time.as_deref()
    }

    pub fn filters(&self) -> Option<&Vec<QueryFilterOrLogic>> {
        self.filters.as_ref()
    }

    pub fn filters_mut(&mut self) -> &mut Vec<QueryFilterOrLogic> {
        self.filters.get_or_insert_with(std::vec::Vec::new)
    }

    pub fn joins(&self) -> Option<&Vec<JoinQuery>> {
        self.joins.as_ref()
    }

    pub fn joins_mut(&mut self) -> Option<&mut Vec<JoinQuery>> {
        self.joins.as_mut()
    }

    pub(crate) fn get_feature_group_by_feature(&self, feature: &Feature) -> Option<&FeatureGroup> {
        let feature_group = self.left_features.iter().find_map(|f| {
            if f.name() == feature.name() {
                Some(&self.left_feature_group)
            } else {
                None
            }
        });
        match feature_group {
            Some(feature_group) => Some(feature_group),
            None => {
                if let Some(joins) = &self.joins {
                    for join in joins {
                        let feature_group = join.query.get_feature_group_by_feature(feature);
                        if feature_group.is_some() {
                            return feature_group;
                        }
                    }
                }
                None
            }
        }
    }

    pub fn feature_groups(&self) -> Vec<&FeatureGroup> {
        if let Some(joins) = &self.joins {
            let mut feature_groups: Vec<&FeatureGroup> = joins
                .iter()
                .map(|join| &join.query.left_feature_group)
                .collect();
            feature_groups.push(&self.left_feature_group);
            feature_groups
        } else {
            vec![&self.left_feature_group]
        }
    }

    pub async fn read_from_online_feature_store(&self) -> Result<DataFrame> {
        read_query_from_online_feature_store(self).await
    }

    pub async fn read_from_offline_feature_store(&self) -> Result<DataFrame> {
        read_with_arrow_flight_client(self.clone()).await
    }

    pub fn join(mut self, query: Query, join_options: JoinOptions) -> Self {
        if self.joins.is_none() {
            self.joins = Some(vec![]);
        }
        self.joins
            .as_mut()
            .unwrap()
            .push(JoinQuery::new(query, join_options));

        self
    }
}

impl From<QueryDTO> for Query {
    fn from(dto: QueryDTO) -> Self {
        Self {
            left_feature_group: FeatureGroup::from(dto.left_feature_group),
            left_features: dto
                .left_features
                .iter()
                .map(|feature_dto| Feature::from(feature_dto.clone()))
                .collect(),
            feature_store_name: dto.feature_store_name.clone(),
            feature_store_id: dto.feature_store_id,
            joins: dto.joins.map(|joins| {
                joins
                    .iter()
                    .map(|join| JoinQuery::from(join.clone()))
                    .collect()
            }),
            filters: dto.filters.map(|filters| {
                filters
                    .iter()
                    .map(|filter| QueryFilterOrLogic::from(filter.clone()))
                    .collect()
            }),
            left_feature_group_start_time: dto.left_feature_group_start_time,
            left_feature_group_end_time: dto.left_feature_group_end_time,
        }
    }
}
