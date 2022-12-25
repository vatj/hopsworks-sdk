use crate::repositories::features::payloads::NewFeaturePayload;

pub fn make_new_feature_payload<'a>(
    name: &'a str,
    data_type: &'a str,
    description: Option<&'a str>,
) -> NewFeaturePayload<'a> {
    NewFeaturePayload::new(name, data_type, description)
}
