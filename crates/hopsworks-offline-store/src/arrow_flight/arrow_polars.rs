use polars::prelude::*;
use arrow::record_batch::RecordBatch;

pub fn record_batch_to_dataframe(batch: &RecordBatch) -> Result<DataFrame, PolarsError> {
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
