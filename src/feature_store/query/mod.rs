//! Query API
pub mod filter;
pub mod join;
pub mod methods;

use serde::{Deserialize, Serialize};

pub use filter::{QueryFilter, QueryFilterOrLogic, QueryLogic};
pub use join::{JoinOptions, JoinQuery};

use crate::{
    feature_store::feature_group::{feature::Feature, FeatureGroup},
    repositories::feature_store::query::entities::QueryDTO,
};

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
