//! Transport from MySQL Source to Arrow Destination.
//!
//! This implementation is taken from the [connector-x](https://github.com/sfu-db/connector-x) crate.
//! The crate itself is added to the Cargo.toml to allow using the core capabilities, but feature flags
//! for src_mysql and dst_arrow are omitted due to the mysql and arrow dependencies being outdated. 
//! The original crate and the source code below are under MIT Licence.


use connectorx::{
    impl_transport,
    typesystem::TypeConversion,
};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use serde_json::{to_string, Value};
use std::marker::PhantomData;
use thiserror::Error;

use crate::sql::mysql2arrow::arrowstream::{
        typesystem::ArrowTypeSystem, ArrowDestination, ArrowDestinationError,
    };

use crate::sql::mysql2arrow::mysql::{
        BinaryProtocol, MySQLSource, MySQLSourceError, MySQLTypeSystem, TextProtocol,
    };


#[derive(Error, Debug)]
pub enum MySQLArrowTransportError {
    #[error(transparent)]
    Source(#[from] MySQLSourceError),

    #[error(transparent)]
    Destination(#[from] ArrowDestinationError),

    #[error(transparent)]
    ConnectorX(#[from] connectorx::errors::ConnectorXError),
}

/// Convert MySQL data types to Arrow data types.
pub struct MySQLArrowTransport<P>(PhantomData<P>);

impl_transport!(
    name = MySQLArrowTransport<BinaryProtocol>,
    error = MySQLArrowTransportError,
    systems = MySQLTypeSystem => ArrowTypeSystem,
    route = MySQLSource<BinaryProtocol> => ArrowDestination,
    mappings = {
        { Float[f32]                 => Float64[f64]            | conversion auto }
        { Double[f64]                => Float64[f64]            | conversion auto }
        { Tiny[i8]                   => Boolean[bool]           | conversion option }
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
        { Time[NaiveTime]            => Time64[NaiveTime]       | conversion auto }
        { Datetime[NaiveDateTime]    => Date64[NaiveDateTime]   | conversion auto }
        { Year[i16]                  => Int64[i64]              | conversion none}
        { Timestamp[NaiveDateTime]   => Date64[NaiveDateTime]   | conversion none }
        { Decimal[Decimal]           => Float64[f64]            | conversion option }
        { VarChar[String]            => LargeUtf8[String]       | conversion auto }
        { Char[String]               => LargeUtf8[String]       | conversion none }
        { Enum[String]               => LargeUtf8[String]       | conversion none }
        { TinyBlob[Vec<u8>]          => LargeBinary[Vec<u8>]    | conversion auto }
        { Blob[Vec<u8>]              => LargeBinary[Vec<u8>]    | conversion none }
        { MediumBlob[Vec<u8>]        => LargeBinary[Vec<u8>]    | conversion none }
        { LongBlob[Vec<u8>]          => LargeBinary[Vec<u8>]    | conversion none }
        { Json[Value]                => LargeUtf8[String]       | conversion option }
    }
);

impl_transport!(
    name = MySQLArrowTransport<TextProtocol>,
    error = MySQLArrowTransportError,
    systems = MySQLTypeSystem => ArrowTypeSystem,
    route = MySQLSource<TextProtocol> => ArrowDestination,
    mappings = {
        { Float[f32]                 => Float64[f64]            | conversion auto }
        { Double[f64]                => Float64[f64]            | conversion auto }
        { Tiny[i8]                   => Boolean[bool]           | conversion option }
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
        { Time[NaiveTime]            => Time64[NaiveTime]       | conversion auto }
        { Datetime[NaiveDateTime]    => Date64[NaiveDateTime]   | conversion auto }
        { Year[i16]                  => Int64[i64]              | conversion none}
        { Timestamp[NaiveDateTime]   => Date64[NaiveDateTime]   | conversion none }
        { Decimal[Decimal]           => Float64[f64]            | conversion option }
        { VarChar[String]            => LargeUtf8[String]       | conversion auto }
        { Char[String]               => LargeUtf8[String]       | conversion none }
        { Enum[String]               => LargeUtf8[String]       | conversion none }
        { TinyBlob[Vec<u8>]          => LargeBinary[Vec<u8>]    | conversion auto }
        { Blob[Vec<u8>]              => LargeBinary[Vec<u8>]    | conversion none }
        { MediumBlob[Vec<u8>]        => LargeBinary[Vec<u8>]    | conversion none }
        { LongBlob[Vec<u8>]          => LargeBinary[Vec<u8>]    | conversion none }
        { Json[Value]                => LargeUtf8[String]       | conversion option }
    }
);


impl<P> TypeConversion<Decimal, f64> for MySQLArrowTransport<P> {
    fn convert(val: Decimal) -> f64 {
        val.to_f64()
            .unwrap_or_else(|| panic!("cannot convert decimal {:?} to float64", val))
    }
}

impl<P> TypeConversion<Value, String> for MySQLArrowTransport<P> {
    fn convert(val: Value) -> String {
        to_string(&val).unwrap()
    }
}

impl<P> TypeConversion<i8, bool> for MySQLArrowTransport<P> {
    fn convert(val: i8) -> bool {
        val != 0
    }
}

