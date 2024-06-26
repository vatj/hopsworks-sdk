use connectorx::impl_typesystem;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

#[derive(Debug, Clone, Copy)]
pub struct DateTimeWrapperMicro(pub DateTime<Utc>);

#[derive(Debug, Clone, Copy)]
pub struct NaiveTimeWrapperMicro(pub NaiveTime);

#[derive(Debug, Clone, Copy)]
pub struct NaiveDateTimeWrapperMicro(pub NaiveDateTime);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PolarsArrowTypeSystem {
    Int32(bool),
    Int64(bool),
    UInt32(bool),
    UInt64(bool),
    Float32(bool),
    Float64(bool),
    Boolean(bool),
    LargeUtf8(bool),
    LargeBinary(bool),
    Date32(bool),
    Date64(bool),
    Date64Micro(bool),
    Time64(bool),
    Time64Micro(bool),
    DateTimeTz(bool),
    DateTimeTzMicro(bool),
    BoolArray(bool),
    Int32Array(bool),
    Int64Array(bool),
    UInt32Array(bool),
    UInt64Array(bool),
    Float32Array(bool),
    Float64Array(bool),
    Utf8Array(bool),
}

impl_typesystem! {
    system = PolarsArrowTypeSystem,
    mappings = {
        { Int32           => i32           }
        { Int64           => i64           }
        { UInt32          => u32           }
        { UInt64          => u64           }
        { Float64         => f64           }
        { Float32         => f32           }
        { Boolean         => bool          }
        { LargeUtf8       => String        }
        { LargeBinary     => Vec<u8>       }
        { Date32          => NaiveDate     }
        { Date64          => NaiveDateTime }
        { Date64Micro     => NaiveDateTimeWrapperMicro }
        { Time64          => NaiveTime     }
        { Time64Micro     => NaiveTimeWrapperMicro }
        { DateTimeTz      => DateTime<Utc> }
        { DateTimeTzMicro => DateTimeWrapperMicro }
        { BoolArray       => Vec<bool>     }
        { Int32Array      => Vec<i32>      }
        { Int64Array      => Vec<i64>      }
        { UInt32Array     => Vec<u32>      }
        { UInt64Array     => Vec<u64>      }
        { Float32Array    => Vec<f32>      }
        { Float64Array    => Vec<f64>      }
        { Utf8Array       => Vec<String>   }
    }
}
