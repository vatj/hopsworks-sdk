use std::sync::Arc;
use color_eyre::Result;

use arrow::record_batch::RecordBatch;
use arrow::datatypes::Schema;

use hopsworks_core::feature_store::FeatureGroup;
use hopsworks_online_store::sql::read_to_arrow::read_query_from_online_feature_store;

pub async fn read_arrow_from_online_store_via_sql(fg: &FeatureGroup) -> Result<(Vec<RecordBatch>, Arc<Schema>)> {
    let query = fg.select_all();
    read_query_from_online_feature_store(&query, None).await
}



