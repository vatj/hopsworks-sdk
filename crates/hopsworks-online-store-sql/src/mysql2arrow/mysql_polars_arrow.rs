//! Transport from MySQL Source to Polars Arrow Destination (based on Arrow2 implementation).
//!
//! This implementation is taken from the [connector-x](https://github.com/sfu-db/connector-x) crate.
//! The crate itself is added to the Cargo.toml to allow using the core capabilities, but feature flags
//! for src_mysql and dst_arrow are omitted due to the mysql and arrow dependencies being outdated.
//! The original crate and the source code below are under MIT Licence.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use connectorx::{impl_transport, typesystem::TypeConversion};
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use serde_json::{to_string, Value};
use std::marker::PhantomData;
use thiserror::Error;

use super::mysql::{BinaryProtocol, MySQLSource, MySQLSourceError, MySQLTypeSystem, TextProtocol};
use super::polars_arrow::{
    typesystem::{NaiveDateTimeWrapperMicro, NaiveTimeWrapperMicro, PolarsArrowTypeSystem},
    PolarsArrowDestination, PolarsArrowDestinationError,
};

#[derive(Error, Debug)]
pub enum MySQLPolarsArrowTransportError {
    #[error(transparent)]
    Source(#[from] MySQLSourceError),

    #[error(transparent)]
    Destination(#[from] PolarsArrowDestinationError),

    #[error(transparent)]
    ConnectorX(#[from] connectorx::errors::ConnectorXError),
}

/// Convert MySQL data types to PolarsArrow data types.
pub struct MySQLPolarsArrowTransport<P>(PhantomData<P>);

impl_transport!(
    name = MySQLPolarsArrowTransport<BinaryProtocol>,
    error = MySQLPolarsArrowTransportError,
    systems = MySQLTypeSystem => PolarsArrowTypeSystem,
    route = MySQLSource<BinaryProtocol> => PolarsArrowDestination,
    mappings = {
        { Float[f32]                 => Float64[f64]            | conversion auto }
        { Double[f64]                => Float64[f64]            | conversion auto }
        { Tiny[i8]                   => Int64[i64]              | conversion auto }
        { Short[i16]                 => Int64[i64]              | conversion auto }
        { Int24[i32]                 => Int64[i64]              | conversion none }
        { Long[i32]                  => Int64[i64]              | conversion auto }
        { LongLong[i64]              => Int64[i64]              | conversion auto }
        { UTiny[u8]                  => Int64[i64]              | conversion auto }
        { UShort[u16]                => Int64[i64]              | conversion auto }
        { ULong[u32]                 => Int64[i64]              | conversion auto }
        { UInt24[u32]                => Int64[i64]              | conversion none }
        { ULongLong[u64]             => Float64[f64]            | conversion auto }
        { Date[NaiveDate]            => Date32[NaiveDate]       | conversion auto }
        { Time[NaiveTime]            => Time64Micro[NaiveTimeWrapperMicro]       | conversion option }
        { Datetime[NaiveDateTime]    => Date64Micro[NaiveDateTimeWrapperMicro]   | conversion option }
        { Year[i16]                  => Int64[i64]              | conversion none}
        { Timestamp[NaiveDateTime]   => Date64Micro[NaiveDateTimeWrapperMicro]   | conversion none }
        { Decimal[Decimal]           => Float64[f64]            | conversion option }
        { VarChar[String]            => LargeUtf8[String]       | conversion auto }
        { Char[String]               => LargeUtf8[String]       | conversion none }
        { Enum[String]               => LargeUtf8[String]       | conversion none }
        { Json[Value]                => LargeUtf8[String]       | conversion option }
        { TinyBlob[Vec<u8>]          => LargeBinary[Vec<u8>]    | conversion auto }
        { Blob[Vec<u8>]              => LargeBinary[Vec<u8>]    | conversion none }
        { MediumBlob[Vec<u8>]        => LargeBinary[Vec<u8>]    | conversion none }
        { LongBlob[Vec<u8>]          => LargeBinary[Vec<u8>]    | conversion none }
    }
);

impl_transport!(
    name = MySQLPolarsArrowTransport<TextProtocol>,
    error = MySQLPolarsArrowTransportError,
    systems = MySQLTypeSystem => PolarsArrowTypeSystem,
    route = MySQLSource<TextProtocol> => PolarsArrowDestination,
    mappings = {
        { Float[f32]                 => Float64[f64]            | conversion auto }
        { Double[f64]                => Float64[f64]            | conversion auto }
        { Tiny[i8]                   => Int64[i64]              | conversion auto }
        { Short[i16]                 => Int64[i64]              | conversion auto }
        { Int24[i32]                 => Int64[i64]              | conversion none }
        { Long[i32]                  => Int64[i64]              | conversion auto }
        { LongLong[i64]              => Int64[i64]              | conversion auto }
        { UTiny[u8]                  => Int64[i64]              | conversion auto }
        { UShort[u16]                => Int64[i64]              | conversion auto }
        { ULong[u32]                 => Int64[i64]              | conversion auto }
        { UInt24[u32]                => Int64[i64]              | conversion none }
        { ULongLong[u64]             => Float64[f64]            | conversion auto }
        { Date[NaiveDate]            => Date32[NaiveDate]       | conversion auto }
        { Time[NaiveTime]            => Time64Micro[NaiveTimeWrapperMicro]       | conversion option }
        { Datetime[NaiveDateTime]    => Date64Micro[NaiveDateTimeWrapperMicro]   | conversion option }
        { Year[i16]                  => Int64[i64]              | conversion none}
        { Timestamp[NaiveDateTime]   => Date64Micro[NaiveDateTimeWrapperMicro]   | conversion none }
        { Decimal[Decimal]           => Float64[f64]            | conversion option }
        { VarChar[String]            => LargeUtf8[String]       | conversion auto }
        { Char[String]               => LargeUtf8[String]       | conversion none }
        { Enum[String]               => LargeUtf8[String]       | conversion none }
        { Json[Value]                => LargeUtf8[String]       | conversion option }
        { TinyBlob[Vec<u8>]          => LargeBinary[Vec<u8>]    | conversion auto }
        { Blob[Vec<u8>]              => LargeBinary[Vec<u8>]    | conversion none }
        { MediumBlob[Vec<u8>]        => LargeBinary[Vec<u8>]    | conversion none }
        { LongBlob[Vec<u8>]          => LargeBinary[Vec<u8>]    | conversion none }
    }
);

impl<P> TypeConversion<NaiveTime, NaiveTimeWrapperMicro> for MySQLPolarsArrowTransport<P> {
    fn convert(val: NaiveTime) -> NaiveTimeWrapperMicro {
        NaiveTimeWrapperMicro(val)
    }
}

impl<P> TypeConversion<NaiveDateTime, NaiveDateTimeWrapperMicro> for MySQLPolarsArrowTransport<P> {
    fn convert(val: NaiveDateTime) -> NaiveDateTimeWrapperMicro {
        NaiveDateTimeWrapperMicro(val)
    }
}

impl<P> TypeConversion<Decimal, f64> for MySQLPolarsArrowTransport<P> {
    fn convert(val: Decimal) -> f64 {
        val.to_f64()
            .unwrap_or_else(|| panic!("cannot convert decimal {:?} to float64", val))
    }
}

impl<P> TypeConversion<Value, String> for MySQLPolarsArrowTransport<P> {
    fn convert(val: Value) -> String {
        to_string(&val).unwrap()
    }
}
