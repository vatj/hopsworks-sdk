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
    pub(crate) fn get_feature_group_by_feature(&self, feature: &Feature) -> Option<FeatureGroup> {
        let feature_group = self.left_features.iter().find_map(|f| {
            if f.name == feature.name {
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

    pub fn feature_groups(&self) -> Vec<FeatureGroup> {
        if let Some(joins) = &self.joins {
            let mut feature_groups: Vec<FeatureGroup> = joins
                .iter()
                .map(|join| join.query.left_feature_group.clone())
                .collect();
            feature_groups.push(self.left_feature_group.clone());
            feature_groups
        } else {
            vec![self.left_feature_group.clone()]
        }
    }

    pub fn add_filters(&mut self, filters: Vec<QueryFilterOrLogic>) {
        if self.filters.is_none() {
            self.filters = Some(vec![]);
        }
        self.filters.as_mut().unwrap().extend(filters);
    }

    pub fn with_filters(mut self, filters: Vec<QueryFilterOrLogic>) -> Self {
        self.add_filters(filters);
        self
    }

    pub async fn read_from_online_feature_store(&self) -> Result<DataFrame> {
        read_query_from_online_feature_store(self).await
    }

    pub async fn read_with_arrow_flight_client(&self) -> Result<DataFrame> {
        read_with_arrow_flight_client(self.clone()).await
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
}
