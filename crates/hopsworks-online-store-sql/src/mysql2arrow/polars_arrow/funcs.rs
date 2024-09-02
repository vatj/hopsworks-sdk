use super::arrow_assoc::ArrowAssoc;
use super::Builder;
use anyhow::anyhow;
use connectorx::errors::Result;
use connectorx::typesystem::{ParameterizedFunc, ParameterizedOn};
use polars_arrow::array::{Array, MutableArray};
use polars_arrow::datatypes::Field;

pub struct FNewBuilder;

impl ParameterizedFunc for FNewBuilder {
    type Function = fn(nrows: usize) -> Builder;
}

impl<T> ParameterizedOn<T> for FNewBuilder
where
    T: ArrowAssoc,
{
    fn parameterize() -> Self::Function {
        fn imp<T>(nrows: usize) -> Builder
        where
            T: ArrowAssoc,
        {
            Box::new(T::builder(nrows)) as Builder
        }
        imp::<T>
    }
}

pub struct FFinishBuilder;

impl ParameterizedFunc for FFinishBuilder {
    type Function = fn(Builder) -> Result<Box<dyn Array>>;
}

impl<T> ParameterizedOn<T> for FFinishBuilder
where
    T: ArrowAssoc,
{
    fn parameterize() -> Self::Function {
        fn imp<T>(mut builder: Builder) -> Result<Box<dyn Array>>
        where
            T: ArrowAssoc,
        {
            builder.shrink_to_fit();
            Ok(MutableArray::as_box(
                builder
                    .as_mut_any()
                    .downcast_mut::<T::Builder>()
                    .ok_or_else(|| anyhow!("cannot cast arrow builder for finish"))?,
            ))
        }
        imp::<T>
    }
}

pub struct FNewField;

impl ParameterizedFunc for FNewField {
    type Function = fn(header: &str) -> Field;
}

impl<T> ParameterizedOn<T> for FNewField
where
    T: ArrowAssoc,
{
    fn parameterize() -> Self::Function {
        fn imp<T>(header: &str) -> Field
        where
            T: ArrowAssoc,
        {
            T::field(header)
        }
        imp::<T>
    }
}
