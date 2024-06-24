use crate::sql::mysql2arrow::mysql::{BinaryProtocol as MySQLBinaryProtocol, TextProtocol};
use connectorx::{
    arrow_batch_iter::{ArrowBatchIter, RecordBatchIterator},
    prelude::*,
    sql::CXQuery,
};
use fehler::{throw, throws};
use log::debug;
#[allow(unused_imports)]
use std::sync::Arc;

#[allow(unreachable_code, unreachable_patterns, unused_variables, unused_mut)]
#[throws(ConnectorXOutError)]
pub fn get_arrow(
    source_conn: &SourceConn,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
) -> ArrowDestination {
    let mut destination = ArrowDestination::new();
    let protocol = source_conn.proto.as_str();
    debug!("Protocol: {}", protocol);

    match source_conn.ty {
        SourceType::MySQL => match protocol {
            "binary" => {
                let source =
                    MySQLSource::<MySQLBinaryProtocol>::new(&source_conn.conn[..], queries.len())?;
                let dispatcher = Dispatcher::<_, _, MySQLArrowTransport<MySQLBinaryProtocol>>::new(
                    source,
                    &mut destination,
                    queries,
                    origin_query,
                );
                dispatcher.run()?;
            }
            "text" => {
                let source =
                    MySQLSource::<TextProtocol>::new(&source_conn.conn[..], queries.len())?;
                let dispatcher = Dispatcher::<_, _, MySQLArrowTransport<TextProtocol>>::new(
                    source,
                    &mut destination,
                    queries,
                    origin_query,
                );
                dispatcher.run()?;
            }
            _ => unimplemented!("{} protocol not supported", protocol),
        },
        _ => throw!(ConnectorXOutError::SourceNotSupport(format!(
            "{:?}",
            source_conn.ty
        ))),
    }

    destination
}

#[allow(unreachable_code, unreachable_patterns, unused_variables, unused_mut)]
pub fn new_record_batch_iter(
    source_conn: &SourceConn,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
    batch_size: usize,
) -> Box<dyn RecordBatchIterator> {
    let destination = ArrowStreamDestination::new_with_batch_size(batch_size);
    let protocol = source_conn.proto.as_str();
    debug!("Protocol: {}", protocol);

    match source_conn.ty {
        SourceType::MySQL => match protocol {
            "binary" => {
                let source =
                    MySQLSource::<MySQLBinaryProtocol>::new(&source_conn.conn[..], queries.len())
                        .unwrap();
                let batch_iter =
                    ArrowBatchIter::<_, MySQLArrowStreamTransport<MySQLBinaryProtocol>>::new(
                        source,
                        destination,
                        origin_query,
                        queries,
                    )
                    .unwrap();
                return Box::new(batch_iter);
            }
            "text" => {
                let source =
                    MySQLSource::<TextProtocol>::new(&source_conn.conn[..], queries.len()).unwrap();
                let batch_iter = ArrowBatchIter::<_, MySQLArrowStreamTransport<TextProtocol>>::new(
                    source,
                    destination,
                    origin_query,
                    queries,
                )
                .unwrap();
                return Box::new(batch_iter);
            }
            _ => unimplemented!("{} protocol not supported", protocol),
        },
        _ => {}
    }
    panic!("not supported!");
}
