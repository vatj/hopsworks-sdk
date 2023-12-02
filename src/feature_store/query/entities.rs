use serde::{Deserialize, Serialize};

use crate::{
    api::feature_store::feature_group::{feature::Feature, FeatureGroup},
    repositories::feature_store::query::entities::QueryDTO,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Query {
    pub left_feature_group: FeatureGroup,
    pub left_features: Vec<Feature>,
    pub feature_store_name: String,
    pub feature_store_id: i32,
    pub joins: Option<Vec<JoinQuery>>,
    pub filter: Option<QueryFilterOrLogic>,
}

impl Query {
    pub fn new_no_joins_no_filter(
        left_feature_group: FeatureGroup,
        left_features: Vec<Feature>,
    ) -> Self {
        Self {
            feature_store_name: left_feature_group.featurestore_name.clone(),
            feature_store_id: left_feature_group.featurestore_id,
            left_feature_group,
            left_features,
            joins: Some(vec![]),
            filter: None,
        }
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
            filter: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JoinQuery {
    pub left_feature_group: FeatureGroup,
    pub left_features: Vec<Feature>,
    pub feature_store_name: String,
    pub feature_store_id: i32,
    pub joins: Vec<JoinQuery>,
    pub filter: Option<QueryFilterOrLogic>,
    pub on: Vec<String>,
    pub left_on: Vec<String>,
    pub right_on: Vec<String>,
    pub join_type: String,
}

impl JoinQuery {
    pub fn new(left_feature_group: FeatureGroup, left_features: Vec<Feature>) -> Self {
        Self {
            feature_store_id: left_feature_group.featurestore_id,
            feature_store_name: left_feature_group.featurestore_name.clone(),
            left_feature_group,
            left_features,
            joins: vec![],
            filter: None,
            on: vec![],
            left_on: vec![],
            right_on: vec![],
            join_type: String::from("INNER"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryFilter {
    pub value: String,
    pub condition: String,
    pub feature: Feature,
}

impl QueryFilter {
    pub fn new(value: String, condition: String, feature: Feature) -> Self {
        Self {
            value,
            condition,
            feature,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryLogic {
    #[serde(rename = "type")]
    pub logic_type: String,
    pub left_logic: Option<Box<QueryLogic>>,
    pub right_logic: Option<Box<QueryLogic>>,
    pub left_filter: Option<QueryFilter>,
    pub right_filter: Option<QueryFilter>,
}

pub enum QueryFilterOrLogic {
    Filter(QueryFilter),
    Logic(QueryLogic),
}

impl Serialize for QueryFilterOrLogic {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match *self {
            QueryFilterOrLogic::Filter(ref filter) => filter.serialize(serializer),
            QueryFilterOrLogic::Logic(ref logic) => logic.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for QueryFilterOrLogic {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.is_object() {
            let filter = serde_json::from_value(value.clone());
            if let Ok(filter) = filter {
                return Ok(QueryFilterOrLogic::Filter(filter));
            }
            let logic = serde_json::from_value(value.clone());
            if let Ok(logic) = logic {
                return Ok(QueryFilterOrLogic::Logic(logic));
            }
            Err(serde::de::Error::custom(
                "expected a JSON object for QueryFilterOrLogicArrowFlightPayload",
            ))
        } else {
            Err(serde::de::Error::custom(
                "expected a JSON object for QueryFilterOrLogicArrowFlightPayload",
            ))
        }
    }
}

impl Clone for QueryFilterOrLogic {
    fn clone(&self) -> Self {
        match *self {
            QueryFilterOrLogic::Filter(ref filter) => QueryFilterOrLogic::Filter(filter.clone()),
            QueryFilterOrLogic::Logic(ref logic) => QueryFilterOrLogic::Logic(logic.clone()),
        }
    }
}

impl std::fmt::Debug for QueryFilterOrLogic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            QueryFilterOrLogic::Filter(ref filter) => filter.fmt(f),
            QueryFilterOrLogic::Logic(ref logic) => logic.fmt(f),
        }
    }
}
