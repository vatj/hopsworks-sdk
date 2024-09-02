use color_eyre::Result;
use polars::prelude::{DataType, Schema, TimeUnit};

pub fn extract_features_from_polars_schema(schema: Schema) -> Result<(Vec<String>, Vec<String>)> {
    let feature_names: Vec<String> = schema.iter_names().map(|name| name.to_string()).collect();
    let feature_types: Vec<String> = schema
        .iter_dtypes()
        .map(|dtype| String::from(convert_polars_data_type(dtype)))
        .collect();

    Ok((feature_names, feature_types))
}

pub fn convert_polars_data_type(data_type: &DataType) -> &str {
    // polars to hopsworks data type
    match data_type {
        DataType::Boolean => "boolean",
        DataType::Int8 => "int",
        DataType::Int16 => "int",
        DataType::UInt16 => "int",
        DataType::Int32 => "int",
        DataType::UInt32 => "bigint",
        DataType::Int64 => "bigint",
        DataType::UInt64 => "bigint",
        DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Datetime(TimeUnit::Nanoseconds, _) => "timestamp",
        DataType::Datetime(TimeUnit::Microseconds, _) => "timestamp",
        DataType::Duration(TimeUnit::Nanoseconds) => "bigint",
        DataType::Duration(TimeUnit::Microseconds) => "bigint",
        DataType::Date => "date",
        DataType::String => "string",
        DataType::Categorical(None, _) => "string",
        DataType::Array(_, _) => "binary",
        DataType::List(_) => "binary",
        DataType::Struct(_) => "binary",
        DataType::Binary => "binary",
        _ => panic!("DataType {:?} not supported.", data_type),
    }
}
