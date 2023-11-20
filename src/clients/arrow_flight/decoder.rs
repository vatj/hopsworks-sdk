// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use ahash::AHashMap;
use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::*,
    io::{
        flight::{deserialize_batch, deserialize_dictionary, deserialize_schemas},
        ipc::IpcSchema,
    },
};
use arrow_flight::error::FlightError;
use arrow_format::flight::data::FlightData;
use bytes::Bytes;
use futures::{ready, stream::BoxStream, Stream, StreamExt};
use polars::{
    prelude::{DataFrame, PolarsError},
    series::Series,
};
use std::{
    fmt::Debug,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};
use tonic::metadata::MetadataMap;

#[derive(Debug)]
pub struct LazyTrailers {
    trailers: Arc<Mutex<Option<MetadataMap>>>,
}

impl LazyTrailers {
    /// gRPC trailers that are known at the end of a stream.
    pub fn get(&self) -> Option<MetadataMap> {
        self.trailers.lock().expect("poisoned").clone()
    }
}

pub type Dictionaries = AHashMap<i64, Box<dyn Array>>;
/// Decodes a [Stream] of [`FlightData`] into pola-rs
/// [`DataFrame`]s. This can be used to decode the response from an
/// Arrow Flight server
///
/// # Note
/// To access the lower level Flight messages (e.g. to access
/// [`FlightData::app_metadata`]), you can call [`Self::into_inner`]
/// and use the [`FlightDataDecoder`] directly.
///
/// # Example:
/// ```no_run
/// # async fn f() -> Result<(), arrow_flight::error::FlightError>{
/// # use bytes::Bytes;
/// // make a do_get request
/// use arrow_flight::{
///   error::Result,
///   Ticket,
///   flight_service_client::FlightServiceClient
/// };
/// use hopsworks_rs::clients::arrow_flight::decoder::FlightDataFrameStream;
///
/// use tonic::transport::Channel;
/// use futures::stream::{StreamExt, TryStreamExt};
///
/// let client: FlightServiceClient<Channel> = // make client..
/// # unimplemented!();
///
/// let request = tonic::Request::new(
///   Ticket { ticket: Bytes::new() }
/// );
///
/// // Get a stream of FlightData;
/// let flight_data_stream = client
///   .do_get(request)
///   .await?
///   .into_inner();
///
/// // Decode stream of FlightData to polars DataFrames
/// let dataframe_stream = FlightDataFrameStream::new_from_flight_data(
///   // convert tonic::Status to FlightError
///   flight_data_stream.map_err(|e| e.into())
/// );
///
/// // Read back DataFrames
/// while let Some(df) = dataframe_stream.next().await {
///   match df {
///     Ok(df) => { /* process dataframe */ },
///     Err(e) => { /* handle error */ },
///   };
/// }
///
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct FlightDataFrameStream {
    /// Optional grpc header metadata.
    headers: MetadataMap,

    /// Optional grpc trailer metadata.
    trailers: Option<LazyTrailers>,

    inner: FlightDataDecoder,
}

impl FlightDataFrameStream {
    /// Create a new [`FlightDataFrameStream`] from a decoded stream
    pub fn new(inner: FlightDataDecoder) -> Self {
        Self {
            inner,
            headers: MetadataMap::default(),
            trailers: None,
        }
    }

    /// Create a new [`FlightDataFrameStream`] from a stream of [`FlightData`]
    pub fn new_from_flight_data<S>(inner: S) -> Self
    where
        S: Stream<Item = std::result::Result<arrow_flight::FlightData, tonic::Status>>
            + Send
            + 'static,
    {
        Self {
            inner: FlightDataDecoder::new(inner),
            headers: MetadataMap::default(),
            trailers: None,
        }
    }

    /// Record response headers.
    pub fn with_headers(self, headers: MetadataMap) -> Self {
        Self { headers, ..self }
    }

    /// Record response trailers.
    pub fn with_trailers(self, trailers: LazyTrailers) -> Self {
        Self {
            trailers: Some(trailers),
            ..self
        }
    }

    /// Headers attached to this stream.
    pub fn headers(&self) -> &MetadataMap {
        &self.headers
    }

    /// Trailers attached to this stream.
    ///
    /// Note that this will return `None` until the entire stream is consumed.
    /// Only after calling `next()` returns `None`, might any available trailers be returned.
    pub fn trailers(&self) -> Option<MetadataMap> {
        self.trailers.as_ref().and_then(|trailers| trailers.get())
    }

    /// Has a message defining the schema been received yet?
    #[deprecated = "use schema().is_some() instead"]
    pub fn got_schema(&self) -> bool {
        self.schema().is_some()
    }

    /// Return ipc_schema for the stream, if it has been received
    pub fn ipc_schema(&self) -> Option<&arrow2::io::ipc::IpcSchema> {
        self.inner.ipc_schema()
    }

    /// Return schema for the stream, if it has been received
    pub fn schema(&self) -> Option<&Schema> {
        self.inner.schema()
    }

    /// Consume self and return the wrapped [`FlightDataDecoder`]
    pub fn into_inner(self) -> FlightDataDecoder {
        self.inner
    }
}

impl futures::Stream for FlightDataFrameStream {
    type Item = std::result::Result<DataFrame, tonic::Status>;

    /// Returns the next [`DataFrame`] available in this stream, or `None` if
    /// there are no further results available.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<std::result::Result<DataFrame, tonic::Status>>> {
        loop {
            let had_schema = self.schema().is_some();
            let res = ready!(self.inner.poll_next_unpin(cx));
            match res {
                // Inner exhausted
                None => {
                    return Poll::Ready(None);
                }
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                // translate data
                Some(Ok(data)) => match data.payload {
                    DecodedPayload::Schema(_) if had_schema => {
                        return Poll::Ready(Some(Err(FlightError::protocol(
                            "Unexpectedly saw multiple Schema messages in FlightData stream",
                        )
                        .into())));
                    }
                    DecodedPayload::Schema(_) => {
                        // Need next message, poll inner again
                    }
                    DecodedPayload::DataFrame(datrame) => {
                        return Poll::Ready(Some(Ok(datrame)));
                    }
                    DecodedPayload::None => {
                        // Need next message
                    }
                },
            }
        }
    }
}

/// Wrapper around a stream of [`FlightData`] that handles the details
/// of decoding low level Flight messages into [`Schema`] and
/// [`DataFrame`]s, including details such as dictionaries.
///
/// # Protocol Details
///
/// The client handles flight messages as followes:
///
/// - **None:** This message has no effect. This is useful to
///   transmit metadata without any actual payload.
///
/// - **Schema:** The schema is (re-)set. Dictionaries are cleared and
///   the decoded schema is returned.
///
/// - **Dictionary Batch:** A new dictionary for a given column is registered. An existing
///   dictionary for the same column will be overwritten. This
///   message is NOT visible.
///
/// - **Record Batch:** A dataframe is created based on the current
///   schema and dictionaries. This fails if no schema was transmitted
///   yet.
///
/// All other message types (at the time of writing: e.g. tensor and
/// sparse tensor) lead to an error.
///
/// Example usecases
///
/// 1. Using this low level stream it is possible to receive a steam
/// of RecordBatches in FlightData that have different schemas by
/// handling multiple schema messages separately.
pub struct FlightDataDecoder {
    /// Underlying data stream
    response: BoxStream<'static, std::result::Result<arrow_flight::FlightData, tonic::Status>>,
    /// Decoding state
    state: Option<FlightStreamState>,
    /// Seen the end of the inner stream?
    done: bool,
}

impl Debug for FlightDataDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightDataDecoder")
            .field("response", &"<stream>")
            .field("state", &self.state)
            .field("done", &self.done)
            .finish()
    }
}

impl FlightDataDecoder {
    /// Create a new wrapper around the stream of [`FlightData`]
    pub fn new<S>(response: S) -> Self
    where
        S: Stream<Item = std::result::Result<arrow_flight::FlightData, tonic::Status>>
            + Send
            + 'static,
    {
        Self {
            state: None,
            response: response.boxed(),
            done: false,
        }
    }

    /// Returns the current schema for this stream
    pub fn ipc_schema(&self) -> Option<&IpcSchema> {
        self.state.as_ref().map(|state| &state.ipc_schema)
    }

    pub fn schema(&self) -> Option<&Schema> {
        self.state.as_ref().map(|state| &state.schema)
    }

    /// Extracts flight data from the next message, updating decoding
    /// state as necessary.
    fn extract_message(
        &mut self,
        data: FlightData,
    ) -> std::result::Result<Option<DecodedFlightData>, tonic::Status> {
        use arrow_ipc::MessageHeader;
        let message = arrow_ipc::root_as_message(&data.data_header[..])
            .map_err(|e| FlightError::DecodeError(format!("Error decoding root message: {e}")))?;

        match message.header_type() {
            MessageHeader::NONE => Ok(Some(DecodedFlightData::new_none(data))),
            MessageHeader::Schema => {
                let (schema, ipc_schema) = deserialize_schemas(&data.data_header[..])
                    .map_err(|e| FlightError::DecodeError(format!("Error decoding schema: {e}")))?;
                self.state = Some(FlightStreamState {
                    schema: schema.clone(),
                    dictionaries_by_field: Dictionaries::default(),
                    ipc_schema: ipc_schema.clone(),
                    chunk_buffer: Arc::new(Mutex::new(vec![])),
                });
                Ok(Some(DecodedFlightData::new_schema(data, ipc_schema)))
            }
            MessageHeader::DictionaryBatch => {
                let state = if let Some(state) = self.state.as_mut() {
                    state
                } else {
                    return Err(
                        FlightError::protocol("Received DictionaryBatch prior to Schema").into(),
                    );
                };

                let fields: &[Field] = state.schema.fields.as_slice();

                deserialize_dictionary(
                    &data,
                    fields,
                    &state.ipc_schema,
                    &mut state.dictionaries_by_field,
                )
                .map_err(|e| {
                    FlightError::DecodeError(format!(
                        "Error decoding ipc DictionaryBatch into arrow2 dictionary: {e}"
                    ))
                })?;

                // Updated internal state, but no decoded message
                Ok(None)
            }
            MessageHeader::RecordBatch => {
                let state = if let Some(state) = self.state.as_mut() {
                    state
                } else {
                    return Err(
                        FlightError::protocol("Received RecordBatch prior to Schema").into(),
                    );
                };

                let fields: &[Field] = state.schema.fields.as_slice();
                let chunk = deserialize_batch(
                    &data,
                    fields,
                    &state.ipc_schema,
                    &state.dictionaries_by_field,
                )
                .map_err(|e| {
                    FlightError::DecodeError(format!(
                        "Error decoding ipc RecordBatch into arrow2 chunk: {e}"
                    ))
                })?;
                state
                    .chunk_buffer
                    .lock()
                    .expect("Poisoned mutex in FlightDataDecoder::extract_message")
                    .push(chunk);

                // let locked_mutex = Arc::try_unwrap(std::mem::swap(
                //     state.chunk_buffer,
                //     Arc::new(Mutex::new(vec![])),
                // ))
                // .expect("Arc::try_unwrap failed")
                // .into_inner()
                // .expect("Mutex::into_inner failed");

                let chunk_buffer = std::mem::replace(
                    &mut state.chunk_buffer,
                    Arc::new(Mutex::new(Vec::<Chunk<Box<dyn Array>>>::new())),
                );

                let locked_mutex = Arc::try_unwrap(chunk_buffer)
                    .expect("Arc::try_unwrap failed")
                    .into_inner()
                    .expect("Poisoned mutex in FlightDataDecoder::extract_message");

                Ok(Some(DecodedFlightData::new_data_frame(
                    data,
                    try_from(locked_mutex, fields).expect("try_from failed"),
                )))
            }
            other => {
                let name = other.variant_name().unwrap_or("UNKNOWN");
                Err(FlightError::protocol(format!("Unexpected message: {name}")).into())
            }
        }
    }
}

impl futures::Stream for FlightDataDecoder {
    type Item = std::result::Result<DecodedFlightData, tonic::Status>;
    /// Returns the result of decoding the next [`FlightData`] message
    /// from the server, or `None` if there are no further results
    /// available.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            let res = ready!(self.response.poll_next_unpin(cx));

            return Poll::Ready(match res {
                None => {
                    self.done = true;
                    None // inner is exhausted
                }
                Some(data) => Some(match data {
                    Err(e) => Err(tonic::Status::from_error(e.into())),
                    Ok(data) => {
                        let converted_data = FlightData {
                            data_header: data.data_header.into(),
                            data_body: data.data_body.into(),
                            app_metadata: data.app_metadata.into(),
                            flight_descriptor: None,
                        };
                        match self.extract_message(converted_data) {
                            Ok(Some(extracted)) => Ok(extracted),
                            Ok(None) => continue, // Need next input message
                            Err(e) => Err(tonic::Status::from_error(e.into())),
                        }
                    }
                }),
            });
        }
    }
}

type ChunkBuffer = Arc<Mutex<Vec<Chunk<Box<dyn Array>>>>>;
/// tracks the state needed to reconstruct [`RecordBatch`]es from a
/// streaming flight response.
#[derive(Debug)]
struct FlightStreamState {
    dictionaries_by_field: Dictionaries,
    ipc_schema: arrow2::io::ipc::IpcSchema,
    schema: Schema,
    chunk_buffer: ChunkBuffer,
}

/// FlightData and the decoded payload (Schema, RecordBatch), if any
#[derive(Debug)]
pub struct DecodedFlightData {
    pub inner: FlightData,
    pub payload: DecodedPayload,
}

impl DecodedFlightData {
    pub fn new_none(inner: FlightData) -> Self {
        Self {
            inner,
            payload: DecodedPayload::None,
        }
    }

    pub fn new_schema(inner: FlightData, schema: arrow2::io::ipc::IpcSchema) -> Self {
        Self {
            inner,
            payload: DecodedPayload::Schema(schema),
        }
    }

    pub fn new_data_frame(inner: FlightData, dataframe: DataFrame) -> Self {
        Self {
            inner,
            payload: DecodedPayload::DataFrame(dataframe),
        }
    }

    /// return the metadata field of the inner flight data
    pub fn app_metadata(&self) -> Bytes {
        Bytes::from(self.inner.app_metadata.clone())
    }
}

/// The result of decoding [`FlightData`]
#[derive(Debug)]
pub enum DecodedPayload {
    /// None (no data was sent in the corresponding FlightData)
    None,

    /// A decoded Schema message
    Schema(arrow2::io::ipc::IpcSchema),

    /// A decoded message converted into a pola-rs Dataframe.
    DataFrame(DataFrame),
}

// taken from https://github.com/sfu-db/connector-x/blob/main/connectorx/src/destinations/arrow2/mod.rs#L127
type VecChunk = Vec<Chunk<Box<dyn Array>>>;
fn try_from(chunks: VecChunk, fields: &[Field]) -> std::result::Result<DataFrame, PolarsError> {
    use polars::prelude::NamedFrom;

    let mut series: Vec<Series> = vec![];

    for chunk in chunks.into_iter() {
        let columns_results: std::result::Result<Vec<Series>, PolarsError> = chunk
            .into_arrays()
            .into_iter()
            .zip(fields)
            .map(|(arr, field)| {
                let a = Series::try_from((field.name.as_str(), arr)).map_err(|_| {
                    PolarsError::ComputeError("Couldn't build Series from box".into())
                });
                a
            })
            .collect();

        let columns = columns_results?;

        if series.is_empty() {
            for col in columns.iter() {
                let name = col.name().to_string();
                series.push(Series::new(&name, col));
            }
            continue;
        }

        for (i, col) in columns.into_iter().enumerate() {
            series[i].append(&col)?;
        }
    }

    DataFrame::new(series)
}
