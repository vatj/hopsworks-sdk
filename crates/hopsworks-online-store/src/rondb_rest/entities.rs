use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SingleFeatureVector {
    features: serde_json::Value,
    passed_values: Option<serde_json::Value>,
    status: FeatureVectorStatus,
    metadata: Option<Vec<MetadataFeatureVector>>,
    detailed_status: Option<Vec<DetailedStatus>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BatchFeatureVectors {
    features: Vec<serde_json::Value>,
    passed_values: Option<Vec<serde_json::Value>>,
    status: Vec<FeatureVectorStatus>,
    metadata: Option<Vec<MetadataFeatureVector>>,
    detailed_status: Option<Vec<DetailedStatus>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetadataFeatureVector {
    feature_name: String,
    feature_type: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DetailedStatus {
    feature_group_id: i32,
    status_code: i32,
}


pub enum FeatureVectorStatus {
    COMPLETE,
    MISSING,
    ERROR,
}

impl Serialize for FeatureVectorStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match *self {
            FeatureVectorStatus::COMPLETE => serializer.serialize_str("COMPLETE"),
            FeatureVectorStatus::MISSING => serializer.serialize_str("MISSING"),
            FeatureVectorStatus::ERROR => serializer.serialize_str("ERROR"),
        }
    }
}

impl<'de> Deserialize<'de> for FeatureVectorStatus {
    fn deserialize<D>(deserializer: D) -> Result<FeatureVectorStatus, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.is_string() {
            let status = value.as_str().unwrap();
            match status {
                "COMPLETE" => Ok(FeatureVectorStatus::COMPLETE),
                "MISSING" => Ok(FeatureVectorStatus::MISSING),
                "ERROR" => Ok(FeatureVectorStatus::ERROR),
                _ => Err(serde::de::Error::custom(format!(
                    "unknown FeatureVectorStatus: {}",
                    status
                ))),
            }
        } else {
            Err(serde::de::Error::custom(
                "expected a JSON string for FeatureVectorStatus",
            ))
        }
    }
}

impl Clone for FeatureVectorStatus {
    fn clone(&self) -> Self {
        match *self {
            FeatureVectorStatus::COMPLETE => FeatureVectorStatus::COMPLETE,
            FeatureVectorStatus::MISSING => FeatureVectorStatus::MISSING,
            FeatureVectorStatus::ERROR => FeatureVectorStatus::ERROR,
        }
    }
}

impl std::fmt::Debug for FeatureVectorStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            FeatureVectorStatus::COMPLETE => write!(f, "FeatureVectorStatus::COMPLETE"),
            FeatureVectorStatus::MISSING => write!(f, "FeatureVectorStatus::MISSING"),
            FeatureVectorStatus::ERROR => write!(f, "FeatureVectorStatus::ERROR"),
        }
    }
}

impl std::fmt::Display for FeatureVectorStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            FeatureVectorStatus::COMPLETE => write!(f, "COMPLETE"),
            FeatureVectorStatus::MISSING => write!(f, "MISSING"),
            FeatureVectorStatus::ERROR => write!(f, "ERROR"),
        }
    }
}



