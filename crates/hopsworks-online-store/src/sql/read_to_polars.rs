use connectorx::prelude::{Arrow2Destination, Dispatcher, MySQLSource};
use connectorx::sources::mysql::BinaryProtocol;
use connectorx::sql::CXQuery;
use connectorx::transports::MySQLArrow2Transport;


pub async fn read_query_from_online_feature_store(
    query: &Query,
    online_read_options: Option<OnlineReadOptions>,
) -> Result<DataFrame> {
    let _online_read_options = online_read_options.unwrap_or_default();
    let connection_string = build_mysql_connection_url_from_storage_connector(
        query.left_feature_group().feature_store_id(),
    )
    .await?;
    let builder = MySQLSource::<BinaryProtocol>::new(connection_string.as_str(), 2).unwrap();

    let constructed_query = construct_query(query).await?;
    let queries = vec![CXQuery::from(&constructed_query.query_online)];
    let mut destination = Arrow2Destination::new();

    let dispatcher = Dispatcher::<
        MySQLSource<BinaryProtocol>,
        Arrow2Destination,
        MySQLArrow2Transport<BinaryProtocol>,
    >::new(builder, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let df: DataFrame = destination.polars().unwrap();

    Ok(df)
}