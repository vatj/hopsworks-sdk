use color_eyre::Result;

use crate::cluster_api::feature_store::feature::payloads::NewFeaturePayload;

pub fn build_feature_payloads_from_schema_and_feature_group_options(
    feature_names: &[String],
    feature_types: &[String],
    primary_key: Vec<&str>,
) -> Result<Vec<NewFeaturePayload>> {
    let mut feature_payloads = feature_names
        .iter()
        .zip(feature_types.iter())
        .map(|(name, data_type)| NewFeaturePayload::new(name.to_string(), data_type.to_string()))
        .collect::<Vec<NewFeaturePayload>>();

    feature_payloads.iter_mut().for_each(|payload| {
        if primary_key.contains(&payload.name.as_str()) {
            payload.primary = true;
        }
    });

    Ok(feature_payloads)
}
