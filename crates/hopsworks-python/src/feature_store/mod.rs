pub mod feature_store;
pub mod feature_group;
pub mod feature_view;
pub mod query;

pub(crate) fn register_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_class::<feature_store::FeatureStore>()?;
    parent.add_class::<feature_group::FeatureGroup>()?;
    parent.add_class::<feature_view::FeatureView>()?;
    parent.add_class::<query::Query>()?;

    Ok(())
}

