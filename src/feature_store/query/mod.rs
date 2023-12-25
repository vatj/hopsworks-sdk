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
    pub left_feature_group: FeatureGroup,
    pub left_features: Vec<Feature>,
    feature_store_name: String,
    feature_store_id: i32,
    pub joins: Option<Vec<JoinQuery>>,
    pub filters: Option<Vec<QueryFilterOrLogic>>,
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
        }
    }

    pub fn feature_store_name(&self) -> &str {
        self.feature_store_name.as_ref()
    }

    pub fn feature_store_id(&self) -> i32 {
        self.feature_store_id
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
            joins: Some(vec![]),
            filters: dto.filters,
        }
    }
}
