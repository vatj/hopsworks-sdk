use color_eyre::Result;
use polars::prelude::DataFrame;
use hopsworks_core::feature_store::query::Query;


pub async fn read_from_online_feature_store(
    query: Query,
    online_read_options: Option<OnlineReadOptions>,
) -> Result<DataFrame> {
    read_query_from_online_feature_store(self, online_read_options).await
}