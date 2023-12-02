use color_eyre::Result;
use polars::frame::DataFrame;

use crate::{
    core::feature_store::query::read_query_from_online_feature_store,
    feature_store::feature_group::{feature::Feature, FeatureGroup},
};

use super::entities::{JoinQuery, Query, QueryFilterOrLogic};

impl Query {
    pub(crate) fn get_feature_group_by_feature(&self, feature: Feature) -> Option<FeatureGroup> {
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
}

impl JoinQuery {
    pub(crate) fn get_feature_group_by_feature(&self, feature: Feature) -> Option<FeatureGroup> {
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
