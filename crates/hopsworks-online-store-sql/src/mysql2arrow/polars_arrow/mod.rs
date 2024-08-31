//! Destination implementation for PolarsArrow.
//!
//! This implementation is taken from the [connector-x](https://github.com/sfu-db/connector-x) crate.
//! The crate itself is added to the Cargo.toml to allow using the core capabilities, but feature flags
//! for src_mysql and dst_arrow are omitted due to the mysql and arrow dependencies being outdated.
//! The original crate and the source code below are under MIT Licence.
mod arrow_assoc;
mod errors;
mod funcs;
pub mod typesystem;

use super::constants::RECORD_BATCH_SIZE;
use anyhow::anyhow;
use arrow_assoc::ArrowAssoc;
use connectorx::data_order::DataOrder;
use connectorx::prelude::{Consume, Destination, DestinationPartition};
use connectorx::typesystem::{Realize, TypeAssoc, TypeSystem};
pub use errors::{PolarsArrowDestinationError, Result};
use fehler::throw;
use fehler::throws;
use funcs::{FFinishBuilder, FNewBuilder, FNewField};
use polars::prelude::DataFrame;
use polars_arrow::{
    array::{Array, MutableArray},
    datatypes::ArrowSchema as Schema,
    record_batch::RecordBatch,
};
use polars_core::utils::accumulate_dataframes_vertical;
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};
pub use typesystem::PolarsArrowTypeSystem;

type Builder = Box<dyn MutableArray + 'static + Send>;
type Builders = Vec<Builder>;
type ChunkBuffer = Arc<Mutex<Vec<RecordBatch>>>;

pub struct PolarsArrowDestination {
    schema: Vec<PolarsArrowTypeSystem>,
    names: Vec<String>,
    data: ChunkBuffer,
    arrow_schema: Arc<Schema>,
}

impl Default for PolarsArrowDestination {
    fn default() -> Self {
        PolarsArrowDestination {
            schema: vec![],
            names: vec![],
            data: Arc::new(Mutex::new(vec![])),
            arrow_schema: Arc::new(Schema::default()),
        }
    }
}

impl PolarsArrowDestination {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Destination for PolarsArrowDestination {
    const DATA_ORDERS: &'static [DataOrder] = &[DataOrder::ColumnMajor, DataOrder::RowMajor];
    type TypeSystem = PolarsArrowTypeSystem;
    type Partition<'a> = ArrowPartitionWriter;
    type Error = PolarsArrowDestinationError;

    fn needs_count(&self) -> bool {
        false
    }

    #[throws(PolarsArrowDestinationError)]
    fn allocate<S: AsRef<str>>(
        &mut self,
        _nrows: usize,
        names: &[S],
        schema: &[PolarsArrowTypeSystem],
        data_order: DataOrder,
    ) {
        // todo: support colmajor
        if !matches!(data_order, DataOrder::RowMajor) {
            throw!(connectorx::errors::ConnectorXError::UnsupportedDataOrder(
                data_order
            ))
        }

        // parse the metadata
        self.schema = schema.to_vec();
        self.names = names.iter().map(|n| n.as_ref().to_string()).collect();
        let fields = self
            .schema
            .iter()
            .zip(&self.names)
            .map(|(&dt, h)| Ok(Realize::<FNewField>::realize(dt)?(h.as_str())))
            .collect::<Result<Vec<_>>>()?;
        self.arrow_schema = Arc::new(Schema::from(fields));
    }

    #[throws(PolarsArrowDestinationError)]
    fn partition(&mut self, counts: usize) -> Vec<Self::Partition<'_>> {
        let mut partitions = vec![];
        for _ in 0..counts {
            partitions.push(ArrowPartitionWriter::new(
                self.schema.clone(),
                Arc::clone(&self.data),
            )?);
        }
        partitions
    }

    fn schema(&self) -> &[PolarsArrowTypeSystem] {
        self.schema.as_slice()
    }
}

impl PolarsArrowDestination {
    pub fn arrow_schema(&self) -> Arc<Schema> {
        self.arrow_schema.clone()
    }

    pub fn names(&self) -> &[String] {
        self.names.as_slice()
    }

    #[throws(PolarsArrowDestinationError)]
    pub fn arrow(self) -> Vec<RecordBatch> {
        let lock = Arc::try_unwrap(self.data).map_err(|_| anyhow!("Partitions are not freed"))?;
        lock.into_inner()
            .map_err(|e| anyhow!("mutex poisoned {}", e))?
    }

    #[throws(PolarsArrowDestinationError)]
    pub fn polars(self) -> DataFrame {
        let schema = self.arrow_schema();
        let batches = self.arrow()?;

        accumulate_dataframes_vertical(
            batches
                .into_iter()
                .map(|rb| DataFrame::try_from((rb, schema.fields.as_slice())).unwrap()),
        )?
    }
}

pub struct ArrowPartitionWriter {
    schema: Vec<PolarsArrowTypeSystem>,
    builders: Option<Builders>,
    current_row: usize,
    current_col: usize,
    data: ChunkBuffer,
}

impl ArrowPartitionWriter {
    #[throws(PolarsArrowDestinationError)]
    fn new(schema: Vec<PolarsArrowTypeSystem>, data: ChunkBuffer) -> Self {
        let mut pw = ArrowPartitionWriter {
            schema,
            builders: None,
            current_row: 0,
            current_col: 0,
            data,
        };
        pw.allocate()?;
        pw
    }

    #[throws(PolarsArrowDestinationError)]
    fn allocate(&mut self) {
        let builders = self
            .schema
            .iter()
            .map(|&dt| Ok(Realize::<FNewBuilder>::realize(dt)?(RECORD_BATCH_SIZE)))
            .collect::<Result<Vec<_>>>()?;
        self.builders.replace(builders);
    }

    #[throws(PolarsArrowDestinationError)]
    fn flush(&mut self) {
        let builders = self
            .builders
            .take()
            .unwrap_or_else(|| panic!("arrow builder is none when flush!"));

        let columns = builders
            .into_iter()
            .zip(self.schema.iter())
            .map(|(builder, &dt)| Realize::<FFinishBuilder>::realize(dt)?(builder))
            .collect::<std::result::Result<Vec<Box<dyn Array>>, connectorx::errors::ConnectorXError>>(
            )?;

        let rb = RecordBatch::try_new(columns)?;
        {
            let mut guard = self
                .data
                .lock()
                .map_err(|e| anyhow!("mutex poisoned {}", e))?;
            let inner_data = &mut *guard;
            inner_data.push(rb);
        }
        self.current_row = 0;
        self.current_col = 0;
    }
}

impl<'a> DestinationPartition<'a> for ArrowPartitionWriter {
    type TypeSystem = PolarsArrowTypeSystem;
    type Error = PolarsArrowDestinationError;

    fn ncols(&self) -> usize {
        self.schema.len()
    }

    #[throws(PolarsArrowDestinationError)]
    fn finalize(&mut self) {
        if self.builders.is_some() {
            self.flush()?;
        }
    }

    #[throws(PolarsArrowDestinationError)]
    fn aquire_row(&mut self, _n: usize) -> usize {
        self.current_row
    }
}

impl<'a, T> Consume<T> for ArrowPartitionWriter
where
    T: TypeAssoc<<Self as DestinationPartition<'a>>::TypeSystem> + ArrowAssoc + 'static,
{
    type Error = PolarsArrowDestinationError;

    #[throws(PolarsArrowDestinationError)]
    fn consume(&mut self, value: T) {
        let col = self.current_col;
        self.current_col = (self.current_col + 1) % self.ncols();
        self.schema[col].check::<T>()?;

        match &mut self.builders {
            Some(builders) => {
                <T as ArrowAssoc>::push(
                    builders[col]
                        .as_mut_any()
                        .downcast_mut::<T::Builder>()
                        .ok_or_else(|| anyhow!("cannot cast arrow builder for append"))?,
                    value,
                );
            }
            None => throw!(anyhow!("arrow arrays are empty!")),
        }

        // flush if exceed batch_size
        if self.current_col == 0 {
            self.current_row += 1;
            if self.current_row >= RECORD_BATCH_SIZE {
                self.flush()?;
                self.allocate()?;
            }
        }
    }
}
