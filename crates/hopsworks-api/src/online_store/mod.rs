use std::sync::Arc;
use color_eyre::Result;

use arrow::record_batch::RecordBatch;
use arrow::datatypes::Schema;

use hopsworks_core::feature_store::FeatureGroup;
use hopsworks_online_store::sql::{
    read_to_arrow::read_query_from_online_feature_store,
    read_to_polars::read_polars_from_online_feature_store,
};
use polars::frame::DataFrame;

#[cfg(feature="read_rest_online_store")]
pub mod rest_read;

pub async fn read_arrow_from_online_store_via_sql(fg: &FeatureGroup) -> Result<(Vec<RecordBatch>, Arc<Schema>)> {
    let query = fg.select_all();
    read_query_from_online_feature_store(&query, None).await
}

pub async fn read_polars_from_online_store_via_sql(fg: &FeatureGroup) -> Result<DataFrame> {
    let query = fg.select_all();
    read_polars_from_online_feature_store(&query, None).await
}

#[cfg(feature="blocking")]
pub fn read_arrow_from_online_store_via_sql_blocking(fg: &FeatureGroup, multithreaded: bool) -> Result<(Vec<RecordBatch>, Arc<Schema>)> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();
    
    rt.block_on(read_arrow_from_online_store_via_sql(fg))
}

#[cfg(feature="blocking")]
pub fn read_polars_from_online_store_via_sql_blocking(fg: &FeatureGroup, multithreaded: bool) -> Result<DataFrame> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();
    
    rt.block_on(read_polars_from_online_store_via_sql(fg))
}


