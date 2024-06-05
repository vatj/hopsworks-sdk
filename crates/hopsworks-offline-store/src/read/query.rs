use color_eyre::Result;
use polars::prelude::DataFrame;

use hopsworks_core::feature_store::feature_group::FeatureGroup;
use crate::read::read_options::ArrowFlightReadOptions;
use crate::read::flight_to_polars::read_with_arrow_flight_client;

pub async fn read_from_offline_feature_store(
    fgroup: &FeatureGroup,
    offline_read_options: Option<ArrowFlightReadOptions>,
) -> Result<DataFrame> {

    read_with_arrow_flight_client(fgroup.select(&fgroup.get_feature_names())?, offline_read_options, vec![]).await
}