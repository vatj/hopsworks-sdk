use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::transformation_function::TransformationFunction;
use crate::{api::query::entities::Query, repositories::feature_view::entities::FeatureViewDTO};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FeatureView {
    pub id: i32,
    pub name: String,
    pub version: i32,
    pub query: Query,
    pub transformation_functions: HashMap<String, TransformationFunction>,
    pub feature_store_id: i32,
    pub feature_store_name: String,
}

impl From<FeatureViewDTO> for FeatureView {
    fn from(dto: FeatureViewDTO) -> Self {
        Self {
            id: dto.id,
            name: dto.name,
            version: dto.version,
            query: Query::from(dto.query),
            transformation_functions: HashMap::<String, TransformationFunction>::new(),
            feature_store_id: dto.featurestore_id,
            feature_store_name: dto.featurestore_name,
        }
    }
}
