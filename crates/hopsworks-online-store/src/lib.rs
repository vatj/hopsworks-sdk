use serde::{Deserialize, Serialize};

#[cfg(feature="sql")]
pub mod sql;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct OnlineReadOptions {
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}
