use color_eyre::Result;
use polars::frame::row::Row;
use apache_avro::types::Record;
use apache_avro::Schema;
use polars::prelude::*;

pub(crate) fn convert_df_row_to_avro_record<'a>(
    avro_schema: &'a Schema,
    column_names: &'a [String],
    primary_keys: &'a [String],
    row: &Row<'a>,
) -> Result<(Record<'a>, String)> {
    let mut composite_key: Vec<String> = vec![];
    let mut record = Record::new(avro_schema).unwrap();

    for (value, col_name) in row.0.iter().zip(column_names.iter()) {
        match value.dtype() {
            DataType::Boolean => {
                record.put(col_name, Some(value.try_extract::<i8>()? != 0))
            }
            DataType::Int8 => record.put(col_name, Some(value.try_extract::<i32>()?)),
            DataType::Int16 => record.put(col_name, Some(value.try_extract::<i32>()?)),
            DataType::Int32 => record.put(col_name, Some(value.try_extract::<i32>()?)),
            DataType::Int64 => record.put(col_name, Some(value.try_extract::<i64>()?)),
            DataType::UInt8 => {
                record.put(col_name, Some(value.try_extract::<u8>()? as usize))
            }
            DataType::UInt16 => record.put(
                col_name,
                Some(value.try_extract::<u16>()? as usize),
            ),
            DataType::UInt32 => record.put(
                col_name,
                Some(value.try_extract::<u32>()? as usize),
            ),
            DataType::UInt64 => record.put(
                col_name,
                Some(value.try_extract::<u64>()? as usize),
            ),
            DataType::Duration(TimeUnit::Nanoseconds) => {
                record.put(col_name, Some(value.try_extract::<i64>()?))
            }
            DataType::Duration(TimeUnit::Microseconds) => {
                record.put(col_name, Some(value.try_extract::<i64>()?))
            }
            DataType::Duration(TimeUnit::Milliseconds) => {
                record.put(col_name, Some(value.try_extract::<i32>()?))
            }
            DataType::Float32 => record.put(col_name, Some(value.try_extract::<f32>()?)),
            DataType::Float64 => record.put(col_name, Some(value.try_extract::<f64>()?)),
            DataType::String => record.put(col_name, Some(value.to_string())),
            DataType::Datetime(TimeUnit::Microseconds, None) => {
                record.put(col_name, Some(value.try_extract::<i64>()?))
            }
            DataType::Datetime(TimeUnit::Nanoseconds, None) => {
                record.put(col_name, Some(value.try_extract::<i64>()?))
            }
            DataType::Datetime(TimeUnit::Milliseconds, None) => {
                record.put(col_name, Some(value.try_extract::<i32>()?))
            }
            DataType::Datetime(TimeUnit::Microseconds, Some(_)) => {
                return Err(color_eyre::Report::msg(
                    "Datetime with timezone not supported",
                ));
            }
            DataType::Datetime(TimeUnit::Nanoseconds, Some(_)) => {
                return Err(color_eyre::Report::msg(
                    "Datetime with timezone not supported",
                ));
            }
            DataType::Datetime(TimeUnit::Milliseconds, Some(_)) => {
                return Err(color_eyre::Report::msg(
                    "Datetime with timezone not supported",
                ));
            }
            DataType::Date => record.put(col_name, Some(value.try_extract::<i32>()?)),
            DataType::Time => record.put(col_name, Some(value.try_extract::<i32>()?)),
            DataType::Null => record.put(col_name, None::<()>),
            DataType::Decimal(_, _) => todo!(),
            DataType::Binary => todo!(),
            DataType::Array(_, _) => todo!(),
            DataType::List(_) => todo!(),
            DataType::Categorical(_, _) => todo!(),
            DataType::Struct(_) => todo!(),
            _ => todo!(),
        }

        if primary_keys.contains(col_name) {
            composite_key.push(value.to_string())
        }
    }

    Ok((record, composite_key.join("_")))
}

#[cfg(test)]
mod tests {
    use super::*;
    
  #[tokio::test]
    async fn test_convert_df_row_to_avro_record() {
        // Define your Avro schema based on the expected structure
        let avro_schema = Schema::parse_str(
            "{
                \"type\" : \"record\",
                \"namespace\" : \"df\",
                \"name\" : \"fg\",
                \"fields\" : [
                    { \"name\" : \"i8\" , \"type\" : \"int\" },
                    { \"name\" : \"i16\" , \"type\" : \"int\" },
                    { \"name\" : \"i32\", \"type\": \"int\" },
                    { \"name\" : \"i64\" , \"type\" : \"long\" },
                    { \"name\" : \"u8\" , \"type\" : \"int\" },
                    { \"name\" : \"u16\", \"type\": \"int\" },
                    { \"name\" : \"u32\" , \"type\" : \"int\" },
                    { \"name\" : \"u64\", \"type\": \"long\" },
                    { \"name\" : \"f32\" , \"type\" : \"float\" },
                    { \"name\" : \"f64\" , \"type\" : \"double\" },
                    { \"name\" : \"utf8\" , \"type\" : \"string\" },
                    { \"name\" : \"bool\" , \"type\" : \"boolean\" },
                    { \"name\" : \"date\" , \"type\" : { \"type\" : \"int\", \"logicalType\" : \"date\" } },
                    { \"name\" : \"time\" , \"type\" : { \"type\" : \"int\", \"logicalType\" : \"time-millis\" } },
                    { \"name\" : \"datetime\" , \"type\" : { \"type\" : \"long\", \"logicalType\" : \"timestamp-micros\" } },
                    { \"name\" : \"duration\" , \"type\" : { \"type\" : \"fixed\", \"size\":12, \"name\": \"DataBlockDuration\" } }
                ]
            }",
        )
        .unwrap();

        let col_names = vec![
            "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64", "utf8", "bool",
            "date", "time", "datetime", "duration",
        ];
        let primary_keys = vec!["i64".to_string(), "utf8".to_string()];
        // Create a sample Polars DataFrame row
        let mut df = DataFrame::new(vec![
            Series::new(col_names[0], &[1i8]),
            Series::new(col_names[1], &[1i16]),
            Series::new(col_names[2], &[1i32]),
            Series::new(col_names[3], &[1i64]),
            Series::new(col_names[4], &[1u8]),
            Series::new(col_names[5], &[1u16]),
            Series::new(col_names[6], &[1u32]),
            Series::new(col_names[7], &[1u64]),
            Series::new(col_names[8], &[1f32]),
            Series::new(col_names[9], &[1f64]),
            Series::new(col_names[10], &["test"]),
            Series::new(col_names[11], &[true]),
            Series::new(col_names[12], &[1i32])
                .cast(&DataType::Date)
                .unwrap(),
            Series::new(col_names[13], &[1i32])
                .cast(&DataType::Time)
                .unwrap(),
            Series::new(col_names[14], &[1i64])
                .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
                .unwrap(),
            Series::new(col_names[15], &[1i64])
                .cast(&DataType::Duration(TimeUnit::Microseconds))
                .unwrap(),
        ])
        .unwrap();

        df.as_single_chunk();
        let col_names = df.get_column_names().iter().map(|s| s.to_string()).collect::<Vec<String>>();

        let row = df.get_row(0).unwrap();
        let result = convert_df_row_to_avro_record(&avro_schema, &col_names, &primary_keys, &row);

        assert!(result.is_ok());

        let (avro_record, composite_key) = result.unwrap();

        assert_eq!(composite_key, "1_\"test\"");

        // Assert specific values in the Avro record based on your expectations
        for name in df.get_column_names() {
            assert!(avro_record.fields.iter().any(|field| field.0 == name));
        }
    }
}