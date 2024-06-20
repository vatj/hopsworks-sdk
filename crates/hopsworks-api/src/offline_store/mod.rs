use color_eyre::Result;
use log::debug;
use polars::prelude::DataFrame;
use arrow::record_batch::RecordBatch;

use hopsworks_core::feature_store::FeatureGroup;
use hopsworks_core::feature_store::{FeatureView, query::builder::BatchQueryOptions};
use hopsworks_core::controller::feature_store::feature_view::get_batch_query;

use hopsworks_offline_store::read::{flight_to_polars::read_with_arrow_flight_client, flight_to_record_batch::read_to_record_batch_with_arrow_flight_client};
pub use hopsworks_offline_store::read::read_options::ArrowFlightReadOptions;

/// Reads feature group data from Hopsworks via the Arrow Flight client.
///
/// # Example
/// ```no_run
/// use color_eyre::Result;
///
/// use polars::prelude::*;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///  let project = hopsworks::login(None).await?;
///  let feature_store = project.get_feature_store().await?;
///  
///  let feature_group = feature_store
///    .get_feature_group("my_feature_group", None)
///    .await?
///    .expect("Feature Group not found");
///
///  let df = feature_group.read_from_offline_feature_store(None).await?;
///
///  Ok(())
/// }
/// ```
pub async fn read_from_offline_feature_store(
    fgroup: &FeatureGroup,
    _offline_read_options: Option<ArrowFlightReadOptions>,
) -> Result<DataFrame> {
    let query = fgroup.select(&fgroup.feature_names())?;
    debug!(
        "Reading data from feature group {} with Arrow Flight client",
        fgroup.name()
    );
    let read_df = read_with_arrow_flight_client(query, _offline_read_options, vec![]).await?;

    Ok(read_df)
}

/// Reads feature group data from Hopsworks via the Arrow Flight client.
///
/// # Example
/// ```no_run
/// use color_eyre::Result;
///
/// use polars::prelude::*;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///  let project = hopsworks::login(None).await?;
///  let feature_store = project.get_feature_store().await?;
///  
///  let feature_group = feature_store
///    .get_feature_group("my_feature_group", None)
///    .await?
///    .expect("Feature Group not found");
///
///  let df = feature_group.read_from_offline_feature_store(None).await?;
///
///  Ok(())
/// }
/// ```
pub async fn read_arrow_from_offline_feature_store(
    fgroup: &FeatureGroup,
    _offline_read_options: Option<ArrowFlightReadOptions>,
) -> Result<Vec<RecordBatch>> {
    let query = fgroup.select(&fgroup.feature_names())?;
    debug!(
        "Reading data from feature group {} with Arrow Flight client",
        fgroup.name()
    );
    let read_df = read_to_record_batch_with_arrow_flight_client(query, _offline_read_options, vec![]).await?;

    Ok(read_df)
}


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