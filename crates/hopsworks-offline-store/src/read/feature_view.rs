use color_eyre::Result;
use polars::prelude::DataFrame;

use hopsworks_core::feature_store::{FeatureView, query::builder::BatchQueryOptions};
use hopsworks_core::controller::feature_store::feature_view::get_batch_query;

use super::flight_to_polars::read_with_arrow_flight_client;
use super::read_options::ArrowFlightReadOptions;

pub async fn get_batch_data(
    feature_view: &FeatureView,
    batch_query_options: &BatchQueryOptions,
    offline_read_options: Option<ArrowFlightReadOptions>,
) -> Result<DataFrame> {
    let batch_query = get_batch_query(feature_view, batch_query_options).await?;

    read_with_arrow_flight_client(batch_query, offline_read_options, vec![]).await
}

// Former feature view method
//    #[cfg(feature = "read_arrow_flight_offline_store")]
//     pub async fn get_batch_data(
//         &self,
//         batch_query_options: &BatchQueryOptions,
//         offline_read_options: Option<OfflineReadOptions>,
//     ) -> Result<DataFrame> {
//         crate::controller::feature_store::feature_view::get_batch_data(
//             self,
//             batch_query_options,
//             offline_read_options,
//         )
//         .await
//     }