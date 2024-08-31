use arrow::record_batch::RecordBatch;
use color_eyre::Result;
use connectorx::prelude::Dispatcher;
use connectorx::sql::CXQuery;
use polars::prelude::*;
use polars_core::utils::accumulate_dataframes_vertical;

use hopsworks_core::controller::feature_store::query::{
    build_mysql_connection_url_from_storage_connector, construct_query,
};
use hopsworks_core::feature_store::query::Query;

use crate::mysql2arrow::arrowstream::ArrowDestination;
use crate::mysql2arrow::mysql::{BinaryProtocol, MySQLSource};
use crate::mysql2arrow::mysql_arrowstream::MySQLArrowTransport;
use crate::OnlineReadOptions;

pub async fn read_polars_from_online_feature_store(
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
    let mut destination = ArrowDestination::new();

    let dispatcher = Dispatcher::<
        MySQLSource<BinaryProtocol>,
        ArrowDestination,
        MySQLArrowTransport<BinaryProtocol>,
    >::new(builder, &mut destination, &queries, None);
    dispatcher.run().unwrap();

    let mut dfs = vec![];
    while let Ok(Some(rb)) = destination.record_batch() {
        dfs.push(record_batch_to_dataframe(&rb)?);
    }

    match dfs.is_empty() {
        true => Err(color_eyre::eyre::eyre!("No data returned from the query")),
        false => Ok(accumulate_dataframes_vertical(dfs)?),
    }
}

fn record_batch_to_dataframe(batch: &RecordBatch) -> Result<DataFrame, PolarsError> {
    let schema = batch.schema();
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (i, column) in batch.columns().iter().enumerate() {
        let arrow = Box::<(dyn polars_arrow::array::Array + 'static)>::from(&**column);
        columns.push(Series::from_arrow(
            schema.fields().get(i).unwrap().name(),
            arrow,
        )?);
    }
    Ok(DataFrame::from_iter(columns))
}
