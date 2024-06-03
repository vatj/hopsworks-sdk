use hopsworks_core::controller::feature_store::feature;

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
    feature_group: &FeatureGroup,
    offline_read_options: Option<ArrowFlightReadOptions>,
) -> Result<DataFrame> {
    let query = self.select(&self.get_feature_names())?;
    debug!(
        "Reading data from feature group {} with Arrow Flight client",
        self.name
    );
    let read_df = read_with_arrow_flight_client(query, offline_read_options).await?;

    Ok(read_df)
}