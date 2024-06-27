use std::sync::Arc;
use color_eyre::Result;

use arrow::record_batch::RecordBatch;
use arrow::datatypes::Schema;

use hopsworks_core::feature_store::FeatureGroup;
use hopsworks_online_store::sql::{
    read_to_arrow::read_query_from_online_feature_store,
    read_to_polars::read_polars_from_online_feature_store,
};
#[cfg(feature = "sqlx-feature-vector")]
use hopsworks_online_store::sqlx_feature_vector::{connect, fetch_one_with_many_queries, fetch_all_with_many_queries};

use polars::frame::DataFrame;

pub async fn read_arrow_from_online_store_via_sql(fg: &FeatureGroup) -> Result<(Vec<RecordBatch>, Arc<Schema>)> {
    let query = fg.select_all();
    read_query_from_online_feature_store(&query, None).await
}

pub async fn read_polars_from_online_store_via_sql(fg: &FeatureGroup) -> Result<DataFrame> {
    let query = fg.select_all();
    read_polars_from_online_feature_store(&query, None).await
}

#[cfg(feature = "sqlx-feature-vector")]
pub async fn get_single_feature_vector(queries: &[String]) -> Result<()> {
    fetch_one_with_many_queries(queries).await
}

#[cfg(feature = "sqlx-feature-vector")]
pub async fn get_multiple_feature_vectors(queries: &[String]) -> Result<()> {
    fetch_all_with_many_queries(queries).await
}

#[cfg(feature = "sqlx-feature-vector")]
pub async fn connect_to_mysql_rondb(db_uri: &str) -> Result<()> {
    connect(db_uri).await
}


