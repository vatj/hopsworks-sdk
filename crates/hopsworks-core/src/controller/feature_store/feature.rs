use color_eyre::Result;

#[cfg(feature = "polars")]
use polars::prelude::{DataType, Schema, TimeUnit};

use crate::cluster_api::feature_store::feature::payloads::NewFeaturePayload;

#[cfg(feature = "polars")]
pub fn extract_features_from_polars_schema(schema: Schema) -> Result<Vec<NewFeaturePayload>> {
    Ok(schema
        .iter_fields()
        .map(|field| {
            NewFeaturePayload::new(
                field.name.to_string(),
                String::from(convert_polars_data_type(field.data_type())),
            )
        })
        .collect())
}

#[cfg(feature = "polars")]
pub fn build_feature_payloads_from_schema_and_feature_group_options(
    schema: Schema,
    primary_key: Vec<&str>,
) -> Result<Vec<NewFeaturePayload>> {
    let mut feature_payloads = extract_features_from_polars_schema(schema)?;

    feature_payloads.iter_mut().for_each(|payload| {
        if primary_key.contains(&payload.name.as_str()) {
            payload.primary = true;
        }
    });

    Ok(feature_payloads)
}

#[cfg(feature = "polars")]
pub fn convert_polars_data_type(data_type: &DataType) -> &str {
    // polars to arrow data type
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
        _ => panic!("DataType {:?} not supported.", data_type),
    }
}
