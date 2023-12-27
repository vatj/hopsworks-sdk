use serde::{Deserialize, Serialize};

use crate::repositories::feature_store::query::entities::JoinQueryDTO;

use super::Query;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JoinQuery {
    query: Query,
    on: Option<Vec<String>>,
    left_on: Option<Vec<String>>,
    right_on: Option<Vec<String>>,
    prefix: Option<String>,
    join_type: JoinType,
}

impl JoinQuery {
    pub fn new(query: Query, join_options: JoinOptions) -> Self {
        Self {
            query,
            on: join_options.on,
            left_on: join_options.left_on,
            right_on: join_options.right_on,
            join_type: join_options.join_type,
            prefix: join_options.prefix,
        }
    }

    pub fn query(&self) -> &Query {
        &self.query
    }

    pub fn query_mut(&mut self) -> &mut Query {
        &mut self.query
    }

    pub fn on(&self) -> Option<&[String]> {
        self.on.as_deref()
    }

    pub fn on_mut(&mut self) -> &mut Vec<String> {
        self.on.get_or_insert_with(std::vec::Vec::new)
    }

    pub fn left_on(&self) -> Option<&[String]> {
        self.left_on.as_deref()
    }

    pub fn left_on_mut(&mut self) -> &mut Vec<String> {
        self.left_on.get_or_insert_with(std::vec::Vec::new)
    }

    pub fn right_on(&self) -> Option<&[String]> {
        self.right_on.as_deref()
    }

    pub fn right_on_mut(&mut self) -> &mut Vec<String> {
        self.right_on.get_or_insert_with(std::vec::Vec::new)
    }

    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }
}

impl From<JoinQueryDTO> for JoinQuery {
    fn from(dto: JoinQueryDTO) -> Self {
        Self {
            query: Query::from(dto.query),
            on: dto.on,
            left_on: dto.left_on,
            right_on: dto.right_on,
            join_type: dto.join_type,
            prefix: dto.prefix,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JoinOptions {
    pub on: Option<Vec<String>>,
    pub left_on: Option<Vec<String>>,
    pub right_on: Option<Vec<String>>,
    pub join_type: JoinType,
    pub prefix: Option<String>,
}

impl JoinOptions {
    pub fn new(join_type: JoinType) -> Self {
        Self {
            on: None,
            left_on: None,
            right_on: None,
            join_type,
            prefix: None,
        }
    }

    pub fn with_on(mut self, on: &[&str]) -> Self {
        self.on = Some(on.iter().map(|s| s.to_string()).collect());
        self
    }

    pub fn with_left_on(mut self, left_on: &[&str]) -> Self {
        self.left_on = Some(left_on.iter().map(|s| s.to_string()).collect());
        self
    }

    pub fn with_right_on(mut self, right_on: &[&str]) -> Self {
        self.right_on = Some(right_on.iter().map(|s| s.to_string()).collect());
        self
    }

    pub fn with_join_type(mut self, join_type: JoinType) -> Self {
        self.join_type = join_type;
        self
    }

    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = Some(String::from(prefix));
        self
    }
}

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
