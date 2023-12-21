use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    feature_store::query::{
        logic_filter::{QueryFilterCondition, QueryLogicType},
        JoinQuery, Query,
    },
    repositories::feature_store::{
        feature::entities::FeatureDTO, feature_group::entities::FeatureGroupDTO,
        storage_connector::payloads::FeatureGroupConnectorArrowFlightPayload,
    },
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewQueryPayload<'a> {
    feature_store_name: String,
    feature_store_id: i32,
    left_feature_group: FeatureGroupDTO,
    left_features: Vec<FeatureDTO>,
    left_feature_group_start_time: Option<&'a str>,
    left_feature_group_end_time: Option<&'a str>,
    joins: Option<Vec<NewJoinQueryPayload<'a>>>,
    hive_engine: bool,
    filter: Option<&'a str>,
}

impl<'a> NewQueryPayload<'a> {
    pub fn new(
        left_feature_group: FeatureGroupDTO,
        left_features: Vec<FeatureDTO>,
        left_feature_group_start_time: Option<&'a str>,
        left_feature_group_end_time: Option<&'a str>,
        joins: Option<Vec<NewJoinQueryPayload<'a>>>,
        hive_engine: bool,
        filter: Option<&'a str>,
    ) -> Self {
        Self {
            feature_store_name: left_feature_group.featurestore_name.clone(),
            feature_store_id: left_feature_group.featurestore_id,
            left_feature_group,
            left_features,
            left_feature_group_start_time,
            left_feature_group_end_time,
            joins,
            hive_engine,
            filter,
        }
    }
}

impl<'a> From<Query> for NewQueryPayload<'a> {
    fn from(query: Query) -> Self {
        Self {
            feature_store_name: query.feature_store_name,
            feature_store_id: query.feature_store_id,
            left_feature_group: FeatureGroupDTO::from(query.left_feature_group),
            left_features: query
                .left_features
                .iter()
                .map(|feature| FeatureDTO::from(feature.clone()))
                .collect(),
            left_feature_group_start_time: None,
            left_feature_group_end_time: None,
            joins: query.joins.map(|joins| {
                joins
                    .iter()
                    .map(|join_query| NewJoinQueryPayload::from(join_query.clone()))
                    .collect()
            }),
            hive_engine: true,
            filter: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NewJoinQueryPayload<'a> {
    feature_store_name: String,
    feature_store_id: i32,
    left_feature_group: FeatureGroupDTO,
    left_features: Vec<FeatureDTO>,
    left_feature_group_start_time: Option<&'a str>,
    left_feature_group_end_time: Option<&'a str>,
    joins: Vec<NewJoinQueryPayload<'a>>,
    hive_engine: bool,
    filter: Option<String>,
    on: Vec<String>,
    left_on: Vec<String>,
    right_on: Vec<String>,
    #[serde(rename = "type")]
    join_type: String,
}

impl<'a> NewJoinQueryPayload<'a> {
    pub fn new(
        left_feature_group: FeatureGroupDTO,
        left_features: Vec<FeatureDTO>,
        left_feature_group_start_time: Option<&'a str>,
        left_feature_group_end_time: Option<&'a str>,
        joins: Vec<NewJoinQueryPayload<'a>>,
        hive_engine: bool,
        join_config: PayloadJoinConfig,
    ) -> Self {
        Self {
            feature_store_name: left_feature_group.featurestore_name.clone(),
            feature_store_id: left_feature_group.featurestore_id,
            left_feature_group,
            left_features,
            left_feature_group_start_time,
            left_feature_group_end_time,
            joins,
            hive_engine,
            filter: join_config.filter,
            on: join_config.on,
            left_on: join_config.left_on,
            right_on: join_config.right_on,
            join_type: join_config.join_type,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PayloadJoinConfig {
    pub filter: Option<String>,
    pub on: Vec<String>,
    pub left_on: Vec<String>,
    pub right_on: Vec<String>,
    pub join_type: String,
}

impl PayloadJoinConfig {
    pub fn new(
        filter: Option<String>,
        on: Vec<String>,
        left_on: Vec<String>,
        right_on: Vec<String>,
        join_type: String,
    ) -> Self {
        Self {
            filter,
            on,
            left_on,
            right_on,
            join_type,
        }
    }
}

impl<'a> From<JoinQuery> for NewJoinQueryPayload<'a> {
    fn from(join_query: JoinQuery) -> Self {
        Self {
            feature_store_name: join_query.feature_store_name,
            feature_store_id: join_query.feature_store_id,
            left_feature_group: FeatureGroupDTO::from(join_query.left_feature_group),
            left_features: join_query
                .left_features
                .iter()
                .map(|feature| FeatureDTO::from(feature.clone()))
                .collect(),
            left_feature_group_start_time: None,
            left_feature_group_end_time: None,
            joins: vec![],
            hive_engine: false,
            filter: None,
            on: join_query.on,
            left_on: join_query.left_on,
            right_on: join_query.right_on,
            join_type: join_query.join_type,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryArrowFlightPayload {
    pub query_string: String,
    pub connectors: Option<HashMap<String, FeatureGroupConnectorArrowFlightPayload>>,
    pub filters: Option<Vec<QueryFilterOrLogicArrowFlightPayload>>,
    pub features: HashMap<String, Vec<String>>, // key is feature_group_name, value is vec of feature names
}

impl QueryArrowFlightPayload {
    pub fn new(
        duckdb_query_string: String,
        serialized_feature_names: HashMap<String, Vec<String>>,
        serialized_connectors: Option<HashMap<String, FeatureGroupConnectorArrowFlightPayload>>,
        serialized_filters: Option<Vec<QueryFilterOrLogicArrowFlightPayload>>,
    ) -> Self {
        Self {
            query_string: duckdb_query_string,
            connectors: serialized_connectors,
            filters: serialized_filters,
            features: serialized_feature_names,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryFilterArrowFlightPayload {
    #[serde(rename = "type")]
    pub filter_type: String,
    pub condition: QueryFilterCondition,
    pub value: serde_json::Value,
    pub feature: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryLogicArrowFlightPayload {
    #[serde(rename = "type")]
    pub filter_type: String,
    pub logic_type: QueryLogicType,
    pub left_filter: Option<Box<QueryFilterOrLogicArrowFlightPayload>>,
    pub right_filter: Option<Box<QueryFilterOrLogicArrowFlightPayload>>,
}

impl QueryFilterArrowFlightPayload {
    pub fn new(condition: QueryFilterCondition, value: serde_json::Value, feature: String) -> Self {
        Self {
            filter_type: "filter".to_string(),
            condition,
            value,
            feature,
        }
    }
}

impl QueryLogicArrowFlightPayload {
    pub fn new(
        logic_type: QueryLogicType,
        left_filter: Option<Box<QueryFilterOrLogicArrowFlightPayload>>,
        right_filter: Option<Box<QueryFilterOrLogicArrowFlightPayload>>,
    ) -> Self {
        Self {
            filter_type: "logic".to_string(),
            logic_type,
            left_filter,
            right_filter,
        }
    }
}

pub enum QueryFilterOrLogicArrowFlightPayload {
    Filter(QueryFilterArrowFlightPayload),
    Logic(QueryLogicArrowFlightPayload),
}

impl Serialize for QueryFilterOrLogicArrowFlightPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match *self {
            QueryFilterOrLogicArrowFlightPayload::Filter(ref filter) => {
                filter.serialize(serializer)
            }
            QueryFilterOrLogicArrowFlightPayload::Logic(ref logic) => logic.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for QueryFilterOrLogicArrowFlightPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.is_object() {
            let filter = serde_json::from_value(value.clone());
            if let Ok(filter) = filter {
                return Ok(QueryFilterOrLogicArrowFlightPayload::Filter(filter));
            }
            let logic = serde_json::from_value(value.clone());
            if let Ok(logic) = logic {
                return Ok(QueryFilterOrLogicArrowFlightPayload::Logic(logic));
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

impl Clone for QueryFilterOrLogicArrowFlightPayload {
    fn clone(&self) -> Self {
        match *self {
            QueryFilterOrLogicArrowFlightPayload::Filter(ref filter) => {
                QueryFilterOrLogicArrowFlightPayload::Filter(filter.clone())
            }
            QueryFilterOrLogicArrowFlightPayload::Logic(ref logic) => {
                QueryFilterOrLogicArrowFlightPayload::Logic(logic.clone())
            }
        }
    }
}

impl std::fmt::Debug for QueryFilterOrLogicArrowFlightPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            QueryFilterOrLogicArrowFlightPayload::Filter(ref filter) => filter.fmt(f),
            QueryFilterOrLogicArrowFlightPayload::Logic(ref logic) => logic.fmt(f),
        }
    }
}
