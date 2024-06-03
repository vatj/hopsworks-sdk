pub async fn get_batch_data(
    feature_view: &FeatureView,
    batch_query_options: &BatchQueryOptions,
    offline_read_options: Option<read_option::OfflineReadOptions>,
) -> Result<DataFrame> {
    let batch_query = get_batch_query(feature_view, batch_query_options).await?;

    batch_query
        .read_from_offline_feature_store(offline_read_options)
        .await
}
