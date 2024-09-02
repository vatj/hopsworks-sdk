use serde::{Deserialize, Serialize};

use crate::cluster_api::platform::kafka::KafkaSubjectDTO;
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KafkaSubject {
    pub(crate) schema: String,
    pub(crate) id: i32,
    pub(crate) version: i32,
}

impl From<KafkaSubjectDTO> for KafkaSubject {
    fn from(dto: KafkaSubjectDTO) -> Self {
        Self {
            schema: dto.schema,
            version: dto.version,
            id: dto.id,
        }
    }
}

impl KafkaSubject {
    pub fn schema(&self) -> &str {
        self.schema.as_ref()
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn version(&self) -> i32 {
        self.version
    }
}
