use color_eyre::Result;

use hopsworks_core::feature_store::FeatureView;

#[cfg(feature = "blocking")]
pub fn delete_blocking(fv: &FeatureView, multithreaded: bool) -> Result<()> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();

    rt.block_on(fv.delete())
}
