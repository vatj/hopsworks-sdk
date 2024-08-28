use color_eyre::Result;
use arrow::{record_batch::RecordBatch, datatypes::Schema};
use connectorx::prelude::Dispatcher;
use connectorx::sql::CXQuery;
use std::sync::Arc;

use hopsworks_core::feature_store::query::Query;
use hopsworks_core::controller::feature_store::query::{build_mysql_connection_url_from_storage_connector, construct_query};

use crate::OnlineReadOptions;
use crate::mysql2arrow::arrowstream::ArrowDestination;
use crate::mysql2arrow::mysql::{MySQLSource, BinaryProtocol};
use crate::mysql2arrow::mysql_arrowstream::MySQLArrowTransport;



pub async fn read_query_from_online_feature_store(
    query: &Query,
    online_read_options: Option<OnlineReadOptions>,
) -> Result<(Vec<RecordBatch>, Arc<Schema>)> {
    let _online_read_options = online_read_options.unwrap_or_default();
    let connection_string = build_mysql_connection_url_from_storage_connector(
        query.left_feature_group().feature_store_id(),
    )
    .await?;
    let builder = MySQLSource::<BinaryProtocol>::new(connection_string.as_str(), 2).unwrap();

    let constructed_query = construct_query(query).await?;
    let queries = vec![CXQuery::from(&constructed_query.query_online)];
    let mut destination = ArrowDestination::new();

    let dispatcher = Dispatcher::<
        MySQLSource<BinaryProtocol>,
        ArrowDestination,
        MySQLArrowTransport<BinaryProtocol>,
    >::new(builder, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let schema = destination.arrow_schema();
    let record_batches = destination.arrow()?;

    Ok((record_batches, schema))
}

