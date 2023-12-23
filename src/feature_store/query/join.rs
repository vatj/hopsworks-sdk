use serde::{Deserialize, Serialize};

use super::Query;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JoinQuery {
    pub query: Query,
    pub on: Option<Vec<String>>,
    pub left_on: Option<Vec<String>>,
    pub right_on: Option<Vec<String>>,
    pub prefix: Option<String>,
    pub join_type: JoinType,
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

    pub fn with_on(mut self, on: Vec<String>) -> Self {
        self.on = Some(on);
        self
    }

    pub fn with_left_on(mut self, left_on: Vec<String>) -> Self {
        self.left_on = Some(left_on);
        self
    }

    pub fn with_right_on(mut self, right_on: Vec<String>) -> Self {
        self.right_on = Some(right_on);
        self
    }

    pub fn with_join_type(mut self, join_type: JoinType) -> Self {
        self.join_type = join_type;
        self
    }

    pub fn with_prefix(mut self, prefix: String) -> Self {
        self.prefix = Some(prefix);
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
