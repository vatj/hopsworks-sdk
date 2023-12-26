//! Query API
pub mod filter;
pub mod join;

use color_eyre::Result;
use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};

pub use filter::{QueryFilter, QueryFilterOrLogic, QueryLogic};
pub use join::{JoinOptions, JoinQuery};

use crate::{
    core::feature_store::query::{
        read_query_from_online_feature_store, read_with_arrow_flight_client,
    },
    feature_store::feature_group::{feature::Feature, FeatureGroup},
    repositories::feature_store::query::entities::QueryDTO,
};

use self::read_option::{OfflineReadOptions, OnlineReadOptions};

/// Query object are used to read data from the feature store, both online and offline.
///
/// They are usually constructed by calling `FeatureGroup.select()` and
/// joining with other queries using `Query.join()`.
/// You can subsequently use `Query.read_from_online_feature_store()`
/// or `Query.read_from_offline_feature_store()` to read your Feature data.
/// `Query` objects are not registered in the feature store as such, but are meant to be part of a `FeatureView`.
/// See the [FeatureView][`crate::feature_store::feature_view::FeatureView`] documentation for more details.
///
/// Query objects support:
/// - Joining with other queries
/// - filtering on individual features, see [Feature][`crate::feature_store::feature_group::feature::Feature`]
/// - real-time reads from the online feature store if all features belong to online-enabled [Feature Group][`crate::feature_store::feature_group::FeatureGroup`]s
/// - offline (so-called batch) reads from the offline feature store if all features belong to offline-enabled [Feature Group][`crate::feature_store::feature_group::FeatureGroup`]s
/// - full or per-query time travel, via the `Query.as_of()` method, if all features belong to time travel enabled [Feature Group][`crate::feature_store::feature_group::FeatureGroup`]s
///
/// # Examples
///
/// ## Read some features from a feature group
/// ```no_run
/// # use color_eyre::Result;
/// use hopsworks_rs::hopsworks_login;
///
/// #[tokio::main]
/// pub async fn main() -> Result<()> {
///   let feature_group = hopsworks_login(None).await?.get_feature_store().await?
///     .get_feature_group("my_fg", Some(1)).await?
///     .expect("my_fg not found");
///
///   let query = feature_group.select(&["feature_1", "feature_2"])?;
///   let df = query.read_from_offline_feature_store(None).await?;
///
///   println!("{}", df.head(Some(5)));
///   Ok(())
/// }
/// ```
///
/// ## Join two feature groups to create a Feature View
/// ```no_run
/// # use color_eyre::Result;
/// use hopsworks_rs::{
///   hopsworks_login,
///   feature_store::query::join::{JoinOptions, JoinType},
/// };
///
/// #[tokio::main]
/// pub async fn main() -> Result<()> {
///  let feature_store = hopsworks_login(None).await?.get_feature_store().await?;
///  let fg_1 = feature_store.get_feature_group("my_fg_1", Some(1)).await?.unwrap();
///  let fg_2 = feature_store.get_feature_group("my_fg_2", Some(1)).await?.unwrap();
///
///  let query = fg_1.select(&["feature_1", "feature_2"])?.join(
///    fg_2.select(&["feature_3", "feature_4"])?,
///    JoinOptions::new(JoinType::Inner).with_left_on(&["feature_1"]).with_right_on(&["feature_3"]),
///  );
///
///   let feature_view = feature_store.create_feature_view(
///     "my_feature_view",
///     1,
///     query,
///     None,
///   ).await?;
///
///   Ok(())
/// }
/// ```
///
/// ## Add filters and time travel to a query
/// ```no_run
/// # use color_eyre::Result;
/// use hopsworks_rs::hopsworks_login;
///
/// #[tokio::main]
/// pub async fn main() -> Result<()> {
///   let feature_store = hopsworks_login(None).await?.get_feature_store().await?;
///   let fg_1 = feature_store.get_feature_group("my_fg_1", Some(1)).await?.unwrap();
///
///   let mut query = fg_1.select(&["feature_1", "feature_2"])?.as_of("2024-01-01", "2024-01-02")?;
///   query.filters_mut().extend(
///        vec![
///           fg_1.get_feature("feature_1").unwrap().filter_in(
///             vec![String::from("foo"), String::from("bar")])?,
///           fg_1.get_feature("feature_2").unwrap().filter_eq(42)?,
///       ]
///   );
///
///   let df = query.read_from_offline_feature_store(None).await?;
///   Ok(())
/// }
/// ```
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Query {
    left_feature_group: FeatureGroup,
    left_features: Vec<Feature>,
    feature_store_name: String,
    feature_store_id: i32,
    joins: Option<Vec<JoinQuery>>,
    filters: Option<Vec<QueryFilterOrLogic>>,
    left_feature_group_start_time: Option<String>,
    left_feature_group_end_time: Option<String>,
}

impl Query {
    pub fn new_no_joins_no_filter(
        left_feature_group: FeatureGroup,
        left_features: Vec<Feature>,
    ) -> Self {
        Self {
            feature_store_name: left_feature_group.feature_store_name().to_string(),
            feature_store_id: left_feature_group.feature_store_id(),
            left_feature_group,
            left_features,
            joins: Some(vec![]),
            filters: None,
            left_feature_group_start_time: None,
            left_feature_group_end_time: None,
        }
    }

    pub fn feature_store_name(&self) -> &str {
        self.feature_store_name.as_ref()
    }

    pub fn feature_store_id(&self) -> i32 {
        self.feature_store_id
    }

    pub fn left_feature_group(&self) -> &FeatureGroup {
        &self.left_feature_group
    }

    pub fn left_features(&self) -> &Vec<Feature> {
        &self.left_features
    }

    pub fn left_feature_group_start_time(&self) -> Option<&str> {
        self.left_feature_group_start_time.as_deref()
    }

    pub fn left_feature_group_end_time(&self) -> Option<&str> {
        self.left_feature_group_end_time.as_deref()
    }

    pub fn filters(&self) -> Option<&Vec<QueryFilterOrLogic>> {
        self.filters.as_ref()
    }

    pub fn filters_mut(&mut self) -> &mut Vec<QueryFilterOrLogic> {
        self.filters.get_or_insert_with(std::vec::Vec::new)
    }

    pub fn joins(&self) -> Option<&Vec<JoinQuery>> {
        self.joins.as_ref()
    }

    pub fn joins_mut(&mut self) -> &mut Vec<JoinQuery> {
        self.joins.get_or_insert_with(std::vec::Vec::new)
    }

    pub(crate) fn get_feature_group_by_feature(&self, feature: &Feature) -> Option<&FeatureGroup> {
        let feature_group = self.left_features.iter().find_map(|f| {
            if f.name() == feature.name() {
                Some(&self.left_feature_group)
            } else {
                None
            }
        });
        match feature_group {
            Some(feature_group) => Some(feature_group),
            None => {
                if let Some(joins) = &self.joins {
                    for join in joins {
                        let feature_group = join.query.get_feature_group_by_feature(feature);
                        if feature_group.is_some() {
                            return feature_group;
                        }
                    }
                }
                None
            }
        }
    }

    pub fn feature_groups(&self) -> Vec<&FeatureGroup> {
        if let Some(joins) = &self.joins {
            let mut feature_groups: Vec<&FeatureGroup> = joins
                .iter()
                .map(|join| &join.query.left_feature_group)
                .collect();
            feature_groups.push(&self.left_feature_group);
            feature_groups
        } else {
            vec![&self.left_feature_group]
        }
    }

    pub async fn read_from_online_feature_store(
        &self,
        online_read_options: Option<OnlineReadOptions>,
    ) -> Result<DataFrame> {
        read_query_from_online_feature_store(self, online_read_options).await
    }

    pub async fn read_from_offline_feature_store(
        &self,
        offline_read_options: Option<OfflineReadOptions>,
    ) -> Result<DataFrame> {
        read_with_arrow_flight_client(self.clone(), offline_read_options).await
    }

    pub fn join(mut self, query: Query, join_options: JoinOptions) -> Self {
        if self.joins.is_none() {
            self.joins = Some(vec![]);
        }
        self.joins
            .as_mut()
            .unwrap()
            .push(JoinQuery::new(query, join_options));

        self
    }

    pub fn as_of(mut self, start_time: &str, end_time: &str) -> Result<Self> {
        self.as_of_recursive(start_time, end_time);
        Ok(self)
    }

    fn as_of_recursive(&mut self, start_time: &str, end_time: &str) {
        self.left_feature_group_start_time = Some(start_time.to_string());
        self.left_feature_group_end_time = Some(end_time.to_string());
        self.joins_mut().iter_mut().for_each(|join| {
            join.query.as_of_recursive(start_time, end_time);
        });
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
            joins: dto.joins.map(|joins| {
                joins
                    .iter()
                    .map(|join| JoinQuery::from(join.clone()))
                    .collect()
            }),
            filters: dto.filters.map(|filters| {
                filters
                    .iter()
                    .map(|filter| QueryFilterOrLogic::from(filter.clone()))
                    .collect()
            }),
            left_feature_group_start_time: dto.left_feature_group_start_time,
            left_feature_group_end_time: dto.left_feature_group_end_time,
        }
    }
}

pub mod read_option {
    //! Read options for feature store query
    //!
    //! Placeholder for future functionality, the aim is both to offer more fine grained control over
    //! how data is read from the feature store and to provide sensible defaults when none are specified.
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    pub struct OfflineReadOptions {}

    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    pub struct OnlineReadOptions {}
}
