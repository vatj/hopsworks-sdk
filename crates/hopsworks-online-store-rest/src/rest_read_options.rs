use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::payload::{MetadataOptionsPayload, OptionsPayload};

#[derive(Debug, Clone, Serialize, Deserialize, Default, TypedBuilder)]
pub struct FeatureVectorRestReadOptions {
    #[builder(default = false)]
    feature_name: bool,
    #[builder(default = false)]
    feature_type: bool,
    #[builder(default = false)]
    detailed_status: bool,
    #[builder(default = false)]
    validate_passed_features: bool,
}

impl FeatureVectorRestReadOptions {
    pub(crate) fn to_metadata_options_payload(&self) -> Option<MetadataOptionsPayload> {
        if self.feature_name || self.feature_type {
            Some(MetadataOptionsPayload {
                feature_name: self.feature_name,
                feature_type: self.feature_type,
            })
        } else {
            None
        }
    }

    pub(crate) fn to_options_payload(&self) -> Option<OptionsPayload> {
        if self.detailed_status || self.validate_passed_features {
            Some(OptionsPayload {
                detailed_status: self.detailed_status,
                validate_passed_features: self.validate_passed_features,
            })
        } else {
            None
        }
    }
}
