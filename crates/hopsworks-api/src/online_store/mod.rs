use color_eyre::Result;
use polars::prelude::DataFrame;

use hopsworks_core::feature_store::FeatureGroup;
use hopsworks_online_store::sql::read_to_arrow::read_query_from_online_feature_store;

pub fn read_from_online_store_via_sql(fg: &FeatureGroup) -> Result<DataFrame> {
    let query = fg.select_all()?;
    read_query_from_online_feature_store(&query, None).await
}