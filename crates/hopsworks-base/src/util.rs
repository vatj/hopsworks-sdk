const FEATURE_STORE_NAME_SUFFIX: &str = "_featurestore";

pub fn strip_feature_store_suffix(feature_store_name: &str) -> String {
    feature_store_name
        .to_lowercase()
        .strip_suffix(FEATURE_STORE_NAME_SUFFIX)
        .unwrap_or(feature_store_name)
        .to_owned()
}
