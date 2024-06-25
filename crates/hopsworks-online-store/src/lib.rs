#[cfg(feature="rest")]
pub mod rondb_rest;
#[cfg(feature="sql")]
pub mod sql;

pub struct OnlineReadOptions {
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

impl Default for OnlineReadOptions {
    fn default() -> Self {
        Self {
            limit: None,
            offset: None,
        }
    }
}