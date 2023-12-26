use color_eyre::Result;
use serde::{Deserialize, Serialize};

use crate::{
    feature_store::query::{filter::QueryFilterCondition, QueryFilter, QueryFilterOrLogic},
    repositories::feature_store::feature::entities::FeatureDTO,
};

/// Feature entity gathering metadata about a feature in a feature group.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Feature {
    name: String,
    description: Option<String>,
    data_type: String,
    primary: bool,
    partition: bool,
    hudi_precombine_key: bool,
    feature_group_id: Option<i32>,
}

impl Feature {
    pub fn new_from_dto(feature_dto: FeatureDTO) -> Self {
        Self {
            name: feature_dto.name,
            description: feature_dto.description,
            data_type: feature_dto.data_type,
            primary: feature_dto.primary,
            partition: feature_dto.partition,
            hudi_precombine_key: feature_dto.hudi_precombine_key,
            feature_group_id: feature_dto.feature_group_id,
        }
    }
}

impl From<FeatureDTO> for Feature {
    fn from(feature_dto: FeatureDTO) -> Self {
        Feature::new_from_dto(feature_dto)
    }
}

impl Feature {
    pub fn feature_group_id(&self) -> Option<i32> {
        self.feature_group_id
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn data_type(&self) -> &str {
        self.data_type.as_ref()
    }

    pub fn is_primary(&self) -> bool {
        self.primary
    }

    pub fn is_partition(&self) -> bool {
        self.partition
    }

    pub fn is_hudi_precombine_key(&self) -> bool {
        self.hudi_precombine_key
    }
}

impl Feature {
    /// Create a new filter for this feature. Often used in combination with a query,
    /// to create a new feature view including only the features matching the filter.
    ///
    /// # Arguments
    /// * `pattern` - The pattern to match.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    /// # use hopsworks_rs::hopsworks_login;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let feature_group = hopsworks_login(None).await?
    ///   .get_feature_store().await?
    ///   .get_feature_group("demo_feature_group", Some(1)).await?
    ///   .expect("Feature group not found");
    ///
    /// let mut query = feature_group.select(&["feature_1", "feature_2"])?;
    /// query.filters_mut().extend(
    ///   vec![
    ///     feature_group.get_feature("feature_1").expect("feature_1 not found")
    ///     .filter_like(&String::from("pattern"))?
    ///   ]
    /// );
    /// # Ok(())
    /// # }
    pub fn filter_like(&self, pattern: &str) -> Result<QueryFilterOrLogic> {
        Ok(QueryFilter::new_like(pattern, self.clone())?.into())
    }

    /// Create a new filter for this feature. Often used in combination with a query,
    /// to create a new feature view including only the features matching the filter.
    ///
    /// # Arguments
    /// * `pattern` - The pattern to match.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    /// # use hopsworks_rs::hopsworks_login;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let feature_group = hopsworks_login(None).await?
    ///   .get_feature_store().await?
    ///   .get_feature_group("demo_feature_group", Some(1)).await?
    ///   .expect("Feature group not found");
    ///
    /// let mut query = feature_group.select(&["feature_1", "feature_2"])?;
    /// query.filters_mut().extend(
    ///   vec![
    ///     feature_group.get_feature("feature_1").expect("feature_1 not found")
    ///     .filter_in(vec![String::from("Alice"), String::from("Bob")])?
    ///   ]
    /// );
    /// # Ok(())
    /// # }
    pub fn filter_in<T>(&self, values: Vec<T>) -> Result<QueryFilterOrLogic>
    where
        T: PartialEq + serde::Serialize + serde::de::DeserializeOwned,
    {
        Ok(QueryFilter::new_in(values, self.clone())?.into())
    }

    /// Create a new filter for this feature. Often used in combination with a query,
    /// to create a new feature view including only the features matching the filter.
    ///
    /// # Arguments
    /// * `pattern` - The pattern to match.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    /// # use hopsworks_rs::hopsworks_login;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let feature_group = hopsworks_login(None).await?
    ///   .get_feature_store().await?
    ///   .get_feature_group("demo_feature_group", Some(1)).await?
    ///   .expect("Feature group not found");
    ///
    /// let mut query = feature_group.select(&["feature_1", "feature_2"])?;
    /// query.filters_mut().extend(
    ///   vec![
    ///     feature_group.get_feature("feature_1").expect("feature_1 not found")
    ///     .filter_neq(String::from("Deleted"))?
    ///   ]
    /// );
    /// # Ok(())
    /// # }
    pub fn filter_neq<'a, T>(&self, value: T) -> Result<QueryFilterOrLogic>
    where
        T: 'a + PartialEq + serde::Serialize + serde::de::DeserializeOwned,
    {
        Ok(
            QueryFilter::new_partial_eq(value, QueryFilterCondition::NotEqual, self.clone())?
                .into(),
        )
    }

    /// Create a new filter for this feature. Often used in combination with a query,
    /// to create a new feature view including only the features matching the filter.
    ///
    /// # Arguments
    /// * `pattern` - The pattern to match.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    /// # use hopsworks_rs::hopsworks_login;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let feature_group = hopsworks_login(None).await?
    ///   .get_feature_store().await?
    ///   .get_feature_group("demo_feature_group", Some(1)).await?
    ///   .expect("Feature group not found");
    ///
    /// let mut query = feature_group.select(&["feature_1", "feature_2"])?;
    /// query.filters_mut().extend(
    ///   vec![
    ///     feature_group.get_feature("feature_1").expect("feature_1 not found")
    ///     .filter_eq(String::from("Active"))?
    ///   ]
    /// );
    /// # Ok(())
    /// # }
    pub fn filter_eq<'a, T>(&self, value: T) -> Result<QueryFilterOrLogic>
    where
        T: 'a + PartialEq + serde::Serialize + serde::de::DeserializeOwned,
    {
        Ok(QueryFilter::new_partial_eq(value, QueryFilterCondition::Equal, self.clone())?.into())
    }
    pub fn filter_gt<'a, T>(&self, value: T) -> Result<QueryFilterOrLogic>
    where
        T: 'a + PartialOrd + serde::Serialize + serde::de::DeserializeOwned,
    {
        Ok(
            QueryFilter::new_partial_ord(value, QueryFilterCondition::GreaterThan, self.clone())?
                .into(),
        )
    }

    /// Create a new filter for this feature. Often used in combination with a query,
    /// to create a new feature view including only the features matching the filter.
    ///
    /// # Arguments
    /// * `pattern` - The pattern to match.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    /// # use hopsworks_rs::hopsworks_login;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let feature_group = hopsworks_login(None).await?
    ///   .get_feature_store().await?
    ///   .get_feature_group("demo_feature_group", Some(1)).await?
    ///   .expect("Feature group not found");
    ///
    /// let mut query = feature_group.select(&["feature_1", "feature_2"])?;
    /// query.filters_mut().extend(vec![feature_group.get_feature("feature_1").expect("feature_1 not found").filter_gte(3.)?]);
    /// # Ok(())
    /// # }
    pub fn filter_gte<'a, T>(&self, value: T) -> Result<QueryFilterOrLogic>
    where
        T: 'a + PartialOrd + serde::Serialize + serde::de::DeserializeOwned,
    {
        Ok(QueryFilter::new_partial_ord(
            value,
            QueryFilterCondition::GreaterThanOrEqual,
            self.clone(),
        )?
        .into())
    }

    /// Create a new filter for this feature. Often used in combination with a query,
    /// to create a new feature view including only the features matching the filter.
    ///
    /// # Arguments
    /// * `pattern` - The pattern to match.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    /// # use hopsworks_rs::hopsworks_login;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let feature_group = hopsworks_login(None).await?
    ///   .get_feature_store().await?
    ///   .get_feature_group("demo_feature_group", Some(1)).await?
    ///   .expect("Feature group not found");
    ///
    /// let mut query = feature_group.select(&["feature_1", "feature_2"])?;
    /// query.filters_mut().extend(vec![feature_group.get_feature("feature_1").expect("feature_1 not found").filter_lt(3.)?]);
    /// # Ok(())
    /// # }
    pub fn filter_lt<'a, T>(&self, value: T) -> Result<QueryFilterOrLogic>
    where
        T: 'a + PartialOrd + serde::Serialize + serde::de::DeserializeOwned,
    {
        Ok(
            QueryFilter::new_partial_ord(value, QueryFilterCondition::LessThan, self.clone())?
                .into(),
        )
    }

    /// Create a new filter for this feature. Often used in combination with a query,
    /// to create a new feature view including only the features matching the filter.
    ///
    /// # Arguments
    /// * `pattern` - The pattern to match.
    ///
    /// # Example
    /// ```no_run
    /// # use color_eyre::Result;
    /// # use hopsworks_rs::hopsworks_login;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let feature_group = hopsworks_login(None).await?
    ///   .get_feature_store().await?
    ///   .get_feature_group("demo_feature_group", Some(1)).await?
    ///   .expect("Feature group not found");
    ///
    /// let mut query = feature_group.select(&["feature_1", "feature_2"])?;
    /// query.filters_mut().extend(vec![feature_group.get_feature("feature_1").expect("feature_1 not found").filter_lte(3.)?]);
    /// # Ok(())
    /// # }
    pub fn filter_lte<'a, T>(&self, value: T) -> Result<QueryFilterOrLogic>
    where
        T: 'a + PartialOrd + serde::Serialize + serde::de::DeserializeOwned,
    {
        Ok(QueryFilter::new_partial_ord(
            value,
            QueryFilterCondition::LessThanOrEqual,
            self.clone(),
        )?
        .into())
    }
}
