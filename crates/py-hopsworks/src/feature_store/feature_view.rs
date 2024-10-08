#[cfg(feature = "read_rest_online_store")]
use hopsworks_api::online_store::rest_read::{EntryValuesPayload, PassedValuesPayload};
use indexmap::IndexMap;
use pyo3::{
    prelude::*,
    types::{PyBool, PyDict, PyInt, PyString},
};
use serde::{Deserialize, Serialize};

#[pyclass]
#[repr(transparent)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PyFeatureView {
    pub(crate) fv: hopsworks_api::FeatureView,
}

#[pymethods]
impl PyFeatureView {
    pub fn name(&self) -> String {
        self.fv.name().to_string()
    }

    pub fn version(&self) -> i32 {
        self.fv.version()
    }

    fn delete(&self) -> PyResult<()> {
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        hopsworks_api::blocking::feature_view::delete_blocking(&self.fv, multithreaded)?;
        Ok(())
    }

    #[cfg(feature = "read_rest_online_store")]
    pub fn init_online_store_rest_client(
        &self,
        api_key: String,
        api_version: String,
    ) -> PyResult<()> {
        let api_key = api_key.as_str();
        let api_version = api_version.as_str();
        let reqwest_client = None; // TODO: Add possibility to pass a reqwest client/builder
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        hopsworks_api::online_store::rest_read::init_online_store_rest_client_blocking(
            api_key,
            api_version,
            reqwest_client,
            multithreaded,
        )?;
        Ok(())
    }

    #[cfg(feature = "read_rest_online_store")]
    // #[pyo3(signature=(entry, passed_values=None, rest_read_options=None))]
    pub fn get_feature_vector(
        &self,
        entry: &Bound<PyAny>,
        _passed_values: Option<&Bound<PyAny>>,
        _rest_read_options: Option<&Bound<PyAny>>,
    ) -> PyResult<()> {
        let entry_payload: EntryValuesPayload = entries_to_payload(entry)?;
        let passed_values: Option<PassedValuesPayload> = None;
        let rest_read_options = None;
        let multithreaded = *crate::MULTITHREADED.get().unwrap();
        let sfv = hopsworks_api::online_store::rest_read::get_feature_vector_blocking(
            &self.fv,
            entry_payload,
            passed_values,
            rest_read_options,
            multithreaded,
        )?;
        tracing::info!("{:?}", sfv);
        Ok(())
    }
}

impl From<hopsworks_api::FeatureView> for PyFeatureView {
    fn from(fv: hopsworks_api::FeatureView) -> Self {
        Self { fv }
    }
}

impl From<PyFeatureView> for hopsworks_api::FeatureView {
    fn from(fv: PyFeatureView) -> Self {
        fv.fv
    }
}

fn entries_to_payload(
    entry: &Bound<PyAny>,
) -> PyResult<indexmap::IndexMap<String, serde_json::Value>> {
    let dict = entry.downcast::<PyDict>()?;
    let mut payload: IndexMap<String, serde_json::Value> =
        indexmap::IndexMap::with_capacity(dict.len());
    for (k, v) in dict {
        payload.insert(k.extract()?, value_to_serde_value(&v)?);
    }
    Ok(payload)
}

fn value_to_serde_value(ob: &Bound<PyAny>) -> color_eyre::Result<serde_json::Value> {
    if ob.is_none() {
        return color_eyre::eyre::Ok(serde_json::Value::Null);
    }
    // bool must be checked before int because Python bool is an instance of int.
    else if ob.is_instance_of::<PyBool>() {
        let bool_val = ob.extract()?;
        return color_eyre::eyre::Ok(serde_json::Value::Bool(bool_val));
    } else if ob.is_instance_of::<PyInt>() {
        let number_val: i64 = ob.extract()?;
        return color_eyre::eyre::Ok(serde_json::Value::Number(serde_json::Number::from(
            number_val,
        )));
    } else if ob.is_instance_of::<PyString>() {
        let string_val = ob.extract()?;
        return color_eyre::eyre::Ok(serde_json::Value::String(string_val));
    }

    color_eyre::eyre::bail!("Unsupported type: {:?}", ob)
}
