use crate::api::feature_group::entities::{Feature, FeatureGroup};

use super::entities::{Query, JoinQuery};

impl Query {
    pub(crate) fn get_feature_group_by_feature(&self, feature: Feature) -> Option<FeatureGroup> {
        let feature_group= self.left_features.iter().find_map(|f| {
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
}

impl JoinQuery {
    pub(crate) fn get_feature_group_by_feature(&self, feature: Feature) -> Option<FeatureGroup> {
        let feature_group= self.left_features.iter().find_map(|f| {
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