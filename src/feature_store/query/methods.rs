use color_eyre::Result;
use polars::frame::DataFrame;

use crate::{
    core::feature_store::query::{
        read_query_from_online_feature_store, read_with_arrow_flight_client,
    },
    feature_store::feature_group::{feature::Feature, FeatureGroup},
};

use super::{JoinOptions, JoinQuery, Query, QueryFilterOrLogic};

impl Query {
    pub(crate) fn get_feature_group_by_feature(&self, feature: Feature) -> Option<FeatureGroup> {
        let feature_group = self.left_features.iter().find_map(|f| {
            if f.get_name() == feature.get_name() {
                Some(self.left_feature_group.clone())
            } else {
                None
            }
        });
        match feature_group {
            Some(feature_group) => Some(feature_group),
            None => {
                if let Some(joins) = &self.joins {
                    for join in joins {
                        let feature_group = join.get_feature_group_by_feature(feature.clone());
                        if feature_group.is_some() {
                            return feature_group;
                        }
                    }
                }
                None
            }
        }
    }

    pub fn feature_groups(&self) -> Vec<FeatureGroup> {
        if let Some(joins) = &self.joins {
            let mut feature_groups: Vec<FeatureGroup> = joins
                .iter()
                .map(|join| join.left_feature_group.clone())
                .collect();
            feature_groups.push(self.left_feature_group.clone());
            feature_groups
        } else {
            vec![self.left_feature_group.clone()]
        }
    }

    pub fn filters(&self) -> Vec<QueryFilterOrLogic> {
        let mut filters = vec![];
        if let Some(joins) = &self.joins {
            for join in joins {
                filters.push(join.filter.clone());
            }
        }
        if self.filter.is_some() {
            filters.push(self.filter.clone());
        }
        // Remove None values
        filters.into_iter().flatten().collect()
    }

    pub async fn read_from_online_feature_store(&self) -> Result<DataFrame> {
        read_query_from_online_feature_store(self).await
    }

    pub async fn read_with_arrow_flight_client(&self) -> Result<DataFrame> {
        read_with_arrow_flight_client(self.clone()).await
    }

    pub fn join(mut self, query: Query, join_options: Option<JoinOptions>) -> Self {
        if self.joins.is_none() {
            self.joins = Some(vec![]);
        }
        self.joins
            .as_mut()
            .unwrap()
            .push(query.to_join_query(join_options));

        self
    }

    fn to_join_query(&self, _join_options: Option<JoinOptions>) -> JoinQuery {
        JoinQuery {
            left_feature_group: self.left_feature_group.clone(),
            left_features: self.left_features.clone(),
            feature_store_name: self.feature_store_name.clone(),
            feature_store_id: self.feature_store_id,
            joins: vec![],
            filter: self.filter.clone(),
            on: vec![],
            left_on: vec![],
            right_on: vec![],
            join_type: "inner".to_owned(),
        }
    }
}

impl JoinQuery {
    pub(crate) fn get_feature_group_by_feature(&self, feature: Feature) -> Option<FeatureGroup> {
        let feature_group = self.left_features.iter().find_map(|f| {
            if f.get_name() == feature.get_name() {
                Some(self.left_feature_group.clone())
            } else {
                None
            }
        });
        match feature_group {
            Some(feature_group) => Some(feature_group),
            None => {
                for join in self.joins.clone() {
                    let feature_group = join.get_feature_group_by_feature(feature.clone());
                    if feature_group.is_some() {
                        return feature_group;
                    }
                }
                None
            }
        }
    }
}
