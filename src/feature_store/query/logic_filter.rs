use serde::{Deserialize, Serialize};

use crate::feature_store::feature_group::feature::Feature;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryFilter {
    pub value: String,
    pub condition: QueryFilterCondition,
    pub feature: Feature,
}

impl QueryFilter {
    pub fn new(value: String, condition: QueryFilterCondition, feature: Feature) -> Self {
        Self {
            value,
            condition,
            feature,
        }
    }

    pub fn new_partial_eq<T>(value: T, condition: QueryFilterCondition, feature: Feature) -> Self
    where
        T: PartialEq,
    {
        Self::new(value, QueryFilterCondition::Equal, feature)
    }

    pub fn and(self, other: QueryFilterOrLogic) -> QueryFilterOrLogic {
        match other {
            QueryFilterOrLogic::Filter(filter) => QueryFilterOrLogic::Logic(QueryLogic::new(
                QueryLogicType::And,
                None,
                None,
                Some(self),
                Some(filter),
            )),
            QueryFilterOrLogic::Logic(logic) => QueryFilterOrLogic::Logic(QueryLogic::new(
                QueryLogicType::And,
                None,
                Some(Box::new(logic)),
                Some(self),
                None,
            )),
        }
    }

    pub fn or(self, other: QueryFilterOrLogic) -> QueryFilterOrLogic {
        match other {
            QueryFilterOrLogic::Filter(filter) => QueryFilterOrLogic::Logic(QueryLogic::new(
                QueryLogicType::Or,
                None,
                None,
                Some(self),
                Some(filter),
            )),
            QueryFilterOrLogic::Logic(logic) => QueryFilterOrLogic::Logic(QueryLogic::new(
                QueryLogicType::Or,
                None,
                Some(Box::new(logic)),
                Some(self),
                None,
            )),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryLogic {
    #[serde(rename = "type")]
    pub logic_type: QueryLogicType,
    pub left_logic: Option<Box<QueryLogic>>,
    pub right_logic: Option<Box<QueryLogic>>,
    pub left_filter: Option<QueryFilter>,
    pub right_filter: Option<QueryFilter>,
}

impl QueryLogic {
    pub fn new(
        logic_type: QueryLogicType,
        left_logic: Option<Box<QueryLogic>>,
        right_logic: Option<Box<QueryLogic>>,
        left_filter: Option<QueryFilter>,
        right_filter: Option<QueryFilter>,
    ) -> Self {
        Self {
            logic_type,
            left_logic,
            right_logic,
            left_filter,
            right_filter,
        }
    }

    pub fn and(self, other: QueryFilterOrLogic) -> QueryFilterOrLogic {
        match other {
            QueryFilterOrLogic::Filter(filter) => QueryFilterOrLogic::Logic(QueryLogic::new(
                QueryLogicType::And,
                Some(Box::new(self)),
                None,
                None,
                Some(filter),
            )),
            QueryFilterOrLogic::Logic(logic) => QueryFilterOrLogic::Logic(QueryLogic::new(
                QueryLogicType::And,
                Some(Box::new(self)),
                Some(Box::new(logic)),
                None,
                None,
            )),
        }
    }

    pub fn or(self, other: QueryFilterOrLogic) -> QueryFilterOrLogic {
        match other {
            QueryFilterOrLogic::Filter(filter) => QueryFilterOrLogic::Logic(QueryLogic::new(
                QueryLogicType::Or,
                Some(Box::new(self)),
                None,
                None,
                Some(filter),
            )),
            QueryFilterOrLogic::Logic(logic) => QueryFilterOrLogic::Logic(QueryLogic::new(
                QueryLogicType::Or,
                Some(Box::new(self)),
                Some(Box::new(logic)),
                None,
                None,
            )),
        }
    }
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

#[derive(Debug)]
pub enum QueryFilterCondition {
    GreaterThanOrEqual,
    GreaterThan,
    LessThanOrEqual,
    LessThan,
    Equal,
    NotEqual,
    In,
    Like,
}

impl Serialize for QueryFilterCondition {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match *self {
            QueryFilterCondition::GreaterThanOrEqual => {
                serializer.serialize_str("GREATER_THAN_OR_EQUAL")
            }
            QueryFilterCondition::GreaterThan => serializer.serialize_str("GREATER_THAN"),
            QueryFilterCondition::LessThanOrEqual => serializer.serialize_str("LESS_THAN_OR_EQUAL"),
            QueryFilterCondition::LessThan => serializer.serialize_str("LESS_THAN"),
            QueryFilterCondition::Equal => serializer.serialize_str("EQUAL"),
            QueryFilterCondition::NotEqual => serializer.serialize_str("NOT_EQUAL"),
            QueryFilterCondition::In => serializer.serialize_str("IN"),
            QueryFilterCondition::Like => serializer.serialize_str("LIKE"),
        }
    }
}

impl<'de> Deserialize<'de> for QueryFilterCondition {
    fn deserialize<D>(deserializer: D) -> Result<QueryFilterCondition, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.is_string() {
            let condition = value.as_str().unwrap();
            match condition {
                "GREATER_THAN_OR_EQUAL" => Ok(QueryFilterCondition::GreaterThanOrEqual),
                "GREATER_THAN" => Ok(QueryFilterCondition::GreaterThan),
                "LESS_THAN_OR_EQUAL" => Ok(QueryFilterCondition::LessThanOrEqual),
                "LESS_THAN" => Ok(QueryFilterCondition::LessThan),
                "EQUAL" => Ok(QueryFilterCondition::Equal),
                "NOT_EQUAL" => Ok(QueryFilterCondition::NotEqual),
                "IN" => Ok(QueryFilterCondition::In),
                "LIKE" => Ok(QueryFilterCondition::Like),
                _ => Err(serde::de::Error::custom(format!(
                    "unknown QueryFilterCondition: {}",
                    condition
                ))),
            }
        } else {
            Err(serde::de::Error::custom(
                "expected a JSON string for QueryFilterCondition",
            ))
        }
    }
}

impl Clone for QueryFilterCondition {
    fn clone(&self) -> Self {
        match *self {
            QueryFilterCondition::GreaterThanOrEqual => QueryFilterCondition::GreaterThanOrEqual,
            QueryFilterCondition::GreaterThan => QueryFilterCondition::GreaterThan,
            QueryFilterCondition::LessThanOrEqual => QueryFilterCondition::LessThanOrEqual,
            QueryFilterCondition::LessThan => QueryFilterCondition::LessThan,
            QueryFilterCondition::Equal => QueryFilterCondition::Equal,
            QueryFilterCondition::NotEqual => QueryFilterCondition::NotEqual,
            QueryFilterCondition::In => QueryFilterCondition::In,
            QueryFilterCondition::Like => QueryFilterCondition::Like,
        }
    }
}

impl std::fmt::Display for QueryFilterCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            QueryFilterCondition::GreaterThanOrEqual => write!(f, "GREATER_THAN_OR_EQUAL"),
            QueryFilterCondition::GreaterThan => write!(f, "GREATER_THAN"),
            QueryFilterCondition::LessThanOrEqual => write!(f, "LESS_THAN_OR_EQUAL"),
            QueryFilterCondition::LessThan => write!(f, "LESS_THAN"),
            QueryFilterCondition::Equal => write!(f, "EQUAL"),
            QueryFilterCondition::NotEqual => write!(f, "NOT_EQUAL"),
            QueryFilterCondition::In => write!(f, "IN"),
            QueryFilterCondition::Like => write!(f, "LIKE"),
        }
    }
}

pub enum QueryLogicType {
    And,
    Or,
    Single,
}

impl Serialize for QueryLogicType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match *self {
            QueryLogicType::And => serializer.serialize_str("AND"),
            QueryLogicType::Or => serializer.serialize_str("OR"),
            QueryLogicType::Single => serializer.serialize_str("SINGLE"),
        }
    }
}

impl<'de> Deserialize<'de> for QueryLogicType {
    fn deserialize<D>(deserializer: D) -> Result<QueryLogicType, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.is_string() {
            let logic_type = value.as_str().unwrap();
            match logic_type {
                "AND" => Ok(QueryLogicType::And),
                "OR" => Ok(QueryLogicType::Or),
                "SINGLE" => Ok(QueryLogicType::Single),
                _ => Err(serde::de::Error::custom(format!(
                    "unknown QueryLogicType: {}",
                    logic_type
                ))),
            }
        } else {
            Err(serde::de::Error::custom(
                "expected a JSON string for QueryLogicType",
            ))
        }
    }
}

impl Clone for QueryLogicType {
    fn clone(&self) -> Self {
        match *self {
            QueryLogicType::And => QueryLogicType::And,
            QueryLogicType::Or => QueryLogicType::Or,
            QueryLogicType::Single => QueryLogicType::Single,
        }
    }
}

impl std::fmt::Debug for QueryLogicType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            QueryLogicType::And => write!(f, "QueryLogicType::And"),
            QueryLogicType::Or => write!(f, "QueryLogicType::Or"),
            QueryLogicType::Single => write!(f, "QueryLogicType::Single"),
        }
    }
}

impl std::fmt::Display for QueryLogicType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            QueryLogicType::And => write!(f, "AND"),
            QueryLogicType::Or => write!(f, "OR"),
            QueryLogicType::Single => write!(f, "SINGLE"),
        }
    }
}
