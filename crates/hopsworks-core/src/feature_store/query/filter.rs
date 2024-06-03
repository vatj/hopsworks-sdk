use color_eyre::Result;
use serde::{Deserialize, Serialize};

use crate::feature_store::feature_group::feature::Feature;
use hopsworks_internal::feature_store::query::{
        QueryFilterDTO, QueryFilterOrLogicDTO, QueryLogicDTO, enums::QueryFilterCondition, enums::QueryLogicType,
    };
use hopsworks_internal::feature_store::feature::FeatureDTO;



#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryFilter {
    pub value: serde_json::Value,
    pub condition: QueryFilterCondition,
    pub feature: Feature,
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

pub enum QueryFilterOrLogic {
    Filter(QueryFilter),
    Logic(QueryLogic),
}

impl From<QueryFilterOrLogicDTO> for QueryFilterOrLogic {
    fn from(query_filter_or_logic: QueryFilterOrLogicDTO) -> Self {
        match query_filter_or_logic {
            QueryFilterOrLogicDTO::Logic(logic) => {
                QueryFilterOrLogic::Logic(QueryLogic::from(logic))
            }
            QueryFilterOrLogicDTO::Filter(filter) => {
                QueryFilterOrLogic::Filter(QueryFilter::from(filter))
            }
        }
    }
}

impl From<QueryFilterDTO> for QueryFilter {
    fn from(query_filter: QueryFilterDTO) -> Self {
        QueryFilter::new(
            query_filter.value,
            query_filter.condition,
            Feature::from(query_filter.feature),
        )
    }
}

impl From<QueryLogicDTO> for QueryLogic {
    fn from(query_logic: QueryLogicDTO) -> Self {
        QueryLogic::new(
            query_logic.logic_type,
            query_logic
                .left_logic
                .map(|logic| Box::new(QueryLogic::from(*logic))),
            query_logic
                .right_logic
                .map(|logic| Box::new(QueryLogic::from(*logic))),
            query_logic.left_filter.map(QueryFilter::from),
            query_logic.right_filter.map(QueryFilter::from),
        )
    }
}

impl QueryFilter {
    pub fn new(
        value: serde_json::Value,
        condition: QueryFilterCondition,
        feature: Feature,
    ) -> Self {
        Self {
            value,
            condition,
            feature,
        }
    }

    pub fn new_partial_eq<T>(
        value: T,
        condition: QueryFilterCondition,
        feature: Feature,
    ) -> Result<QueryFilter>
    where
        T: PartialEq + serde::Serialize + serde::de::DeserializeOwned,
    {
        if condition != QueryFilterCondition::Equal && condition != QueryFilterCondition::NotEqual {
            return Err(color_eyre::eyre::eyre!(
                "QueryFilterCondition must be Equal or NotEqual for partial_eq"
            ));
        }
        let value = serde_json::to_value(value)?;
        Ok(Self::new(value, QueryFilterCondition::Equal, feature))
    }

    pub fn new_partial_ord<T>(
        value: T,
        condition: QueryFilterCondition,
        feature: Feature,
    ) -> Result<QueryFilter>
    where
        T: PartialOrd + serde::Serialize + serde::de::DeserializeOwned,
    {
        if condition != QueryFilterCondition::GreaterThan
            && condition != QueryFilterCondition::GreaterThanOrEqual
            && condition != QueryFilterCondition::LessThan
            && condition != QueryFilterCondition::LessThanOrEqual
        {
            return Err(color_eyre::eyre::eyre!(
                "QueryFilterCondition must be GreaterThan, GreaterThanOrEqual, LessThan, or LessThanOrEqual for partial_ord"
            ));
        }
        let value = serde_json::to_value(value)?;
        Ok(Self::new(value, QueryFilterCondition::Equal, feature))
    }

    pub fn new_in<T>(value: Vec<T>, feature: Feature) -> Result<QueryFilter>
    where
        T: PartialEq + serde::Serialize + serde::de::DeserializeOwned,
    {
        let value = serde_json::to_value(value)?;
        Ok(Self::new(value, QueryFilterCondition::In, feature))
    }

    pub fn new_like(value: &str, feature: Feature) -> Result<QueryFilter> {
        let value = serde_json::to_value(value)?;
        Ok(Self::new(value, QueryFilterCondition::Like, feature))
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

impl From<QueryFilter> for QueryFilterOrLogic {
    fn from(filter: QueryFilter) -> Self {
        QueryFilterOrLogic::Filter(filter)
    }
}

impl std::fmt::Display for QueryFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = serde_json::to_string(&self.value).unwrap();
        write!(
            f,
            "QueryFilter({} {} {})",
            self.feature.name(),
            self.condition,
            value
        )
    }
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

impl From<QueryLogic> for QueryFilterOrLogic {
    fn from(logic: QueryLogic) -> Self {
        QueryFilterOrLogic::Logic(logic)
    }
}

impl std::fmt::Display for QueryLogic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(left_logic) = &self.left_logic {
            if let Some(right_logic) = &self.right_logic {
                write!(f, "({} {} {})", left_logic, self.logic_type, right_logic)
            } else if let Some(right_filter) = &self.right_filter {
                write!(f, "({} {} {})", left_logic, self.logic_type, right_filter)
            } else {
                write!(f, "({})", left_logic)
            }
        } else if let Some(left_filter) = &self.left_filter {
            if let Some(right_logic) = &self.right_logic {
                write!(f, "({} {} {})", left_filter, self.logic_type, right_logic)
            } else if let Some(right_filter) = &self.right_filter {
                write!(f, "({} {} {})", left_filter, self.logic_type, right_filter)
            } else {
                write!(f, "({})", left_filter)
            }
        } else {
            write!(f, "QueryLogicType({})", self.logic_type)
        }
    }
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

impl std::fmt::Display for QueryFilterOrLogic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            QueryFilterOrLogic::Filter(ref filter) => filter.fmt(f),
            QueryFilterOrLogic::Logic(ref logic) => logic.fmt(f),
        }
    }
}

impl From<&QueryFilterOrLogic> for QueryFilterOrLogicDTO {
    fn from(query_filter_or_logic: &QueryFilterOrLogic) -> Self {
        match query_filter_or_logic {
            QueryFilterOrLogic::Logic(logic) => QueryFilterOrLogicDTO::Logic(logic.into()),
            QueryFilterOrLogic::Filter(filter) => QueryFilterOrLogicDTO::Filter(filter.into()),
        }
    }
}

impl From<&QueryFilter> for QueryFilterDTO {
    fn from(query_filter: &QueryFilter) -> Self {
        Self {
            feature: FeatureDTO::from(&query_filter.feature),
            condition: query_filter.condition.clone(),
            value: query_filter.value.clone(),
        }
    }
}

impl From<&QueryLogic> for QueryLogicDTO {
    fn from(value: &QueryLogic) -> Self {
        Self {
            logic_type: value.logic_type.clone(),
            left_logic: value
                .left_logic
                .as_deref()
                .map(|left_logic| Box::new(QueryLogicDTO::from(left_logic))),
            right_logic: value
                .right_logic
                .as_deref()
                .map(|right_logic| Box::new(QueryLogicDTO::from(right_logic))),
            left_filter: value.left_filter.as_ref().map(QueryFilterDTO::from),
            right_filter: value.right_filter.as_ref().map(QueryFilterDTO::from),
        }
    }
}