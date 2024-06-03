use color_eyre::Result;
use polars::prelude::DataFrame;

use hopsworks_core::feature_store::feature_group::FeatureGroup;

pub async fn read_from_offline_feature_store(
    feature_group: &FeatureGroup,
    offline_read_options: Option<ArrowFlightReadOptions>,
) -> Result<DataFrame> {
    use hopsworks_core::controller::feature_store::feature;

    read_with_arrow_flight_client(self.clone(), offline_read_options).await
}