use serde::{Deserialize, Serialize};

mod mysql2arrow;
pub mod read;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct OnlineReadOptions {
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}
