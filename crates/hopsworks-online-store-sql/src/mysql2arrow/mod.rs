//! Strip down version of connector-x to use up to date polars/arrow lib.
//!
//! This implementation is taken from the [connector-x](https://github.com/sfu-db/connector-x) crate.
//! The crate itself is added to the Cargo.toml to allow using the core capabilities, but feature flags
//! for src_mysql and dst_arrow are omitted due to the mysql and arrow dependencies being outdated.
//! The original crate and the source code below are under MIT Licence.

pub mod arrow;
pub mod arrowstream;
pub mod constants;
pub mod mysql;
pub mod mysql_arrow;
pub mod mysql_arrowstream;
pub mod mysql_polars_arrow;
pub mod polars_arrow;
