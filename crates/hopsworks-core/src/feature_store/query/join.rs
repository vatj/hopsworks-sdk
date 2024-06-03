use serde::{Deserialize, Serialize};

use crate::cluster_api::feature_store::query::{JoinQueryDTO, QueryDTO, enums::JoinType, payloads::{NewJoinQueryPayload, NewQueryPayload},};

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



impl From<&JoinQuery> for JoinQueryDTO {
    fn from(join_query: &JoinQuery) -> Self {
        Self {
            query: QueryDTO::from(join_query.query()),
            on: join_query.on().map(|slice| slice.to_vec()),
            left_on: join_query.left_on().map(|slice| slice.to_vec()),
            right_on: join_query.right_on().map(|slice| slice.to_vec()),
            join_type: join_query.join_type().clone(),
            prefix: join_query.prefix().map(str::to_string),
        }
    }
}

impl From<&JoinQuery> for NewJoinQueryPayload {
    fn from(join_query: &JoinQuery) -> Self {
        Self {
            query: NewQueryPayload::from(join_query.query()),
            on: join_query.on().map(|slice| slice.to_vec()),
            left_on: join_query.left_on().map(|slice| slice.to_vec()),
            right_on: join_query.right_on().map(|slice| slice.to_vec()),
            join_type: join_query.join_type().clone(),
            prefix: join_query.prefix().map(str::to_string),
        }
    }
}