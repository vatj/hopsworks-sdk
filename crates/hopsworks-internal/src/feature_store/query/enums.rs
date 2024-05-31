use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    LeftSemiJoin,
    Comma,
}

#[derive(PartialEq)]
pub enum QueryLogicType {
    And,
    Or,
    Single,
}

#[derive(PartialEq)]
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


impl std::fmt::Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER"),
            JoinType::Left => write!(f, "LEFT"),
            JoinType::Right => write!(f, "RIGHT"),
            JoinType::Full => write!(f, "FULL"),
            JoinType::Cross => write!(f, "CROSS"),
            JoinType::LeftSemiJoin => write!(f, "LEFT_SEMI_JOIN"),
            JoinType::Comma => write!(f, "COMMA"),
        }
    }
}

impl std::fmt::Debug for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            JoinType::Inner => write!(f, "JoinType::INNER"),
            JoinType::Left => write!(f, "JoinType::LEFT"),
            JoinType::Right => write!(f, "JoinType::RIGHT"),
            JoinType::Full => write!(f, "JoinType::FULL"),
            JoinType::Cross => write!(f, "JoinType::CROSS"),
            JoinType::LeftSemiJoin => write!(f, "JoinType::LEFT_SEMI_JOIN"),
            JoinType::Comma => write!(f, "JoinType::COMMA"),
        }
    }
}

impl Serialize for JoinType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for JoinType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "INNER" => Ok(JoinType::Inner),
            "LEFT" => Ok(JoinType::Left),
            "RIGHT" => Ok(JoinType::Right),
            "FULL" => Ok(JoinType::Full),
            "CROSS" => Ok(JoinType::Cross),
            "LEFT_SEMI_JOIN" => Ok(JoinType::LeftSemiJoin),
            "COMMA" => Ok(JoinType::Comma),
            _ => Err(serde::de::Error::custom(format!("unknown JoinType: {}", s))),
        }
    }
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

impl std::fmt::Debug for QueryFilterCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            QueryFilterCondition::GreaterThanOrEqual => {
                write!(f, "QueryFilterCondition::GreaterThanOrEqual")
            }
            QueryFilterCondition::GreaterThan => write!(f, "QueryFilterCondition::GreaterThan"),
            QueryFilterCondition::LessThanOrEqual => {
                write!(f, "QueryFilterCondition::LessThanOrEqual")
            }
            QueryFilterCondition::LessThan => write!(f, "QueryFilterCondition::LessThan"),
            QueryFilterCondition::Equal => write!(f, "QueryFilterCondition::Equal"),
            QueryFilterCondition::NotEqual => write!(f, "QueryFilterCondition::NotEqual"),
            QueryFilterCondition::In => write!(f, "QueryFilterCondition::In"),
            QueryFilterCondition::Like => write!(f, "QueryFilterCondition::Like"),
        }
    }
}