use color_eyre::Result;

use hopsworks_core::feature_store::FeatureStore;
use hopsworks_core::Project;

#[cfg(feature = "blocking")]
pub fn get_feature_store_blocking(project: &Project, multithreaded: bool) -> Result<FeatureStore> {
    let rt = hopsworks_core::get_hopsworks_runtime(multithreaded).clone();
    let _guard = rt.enter();

    rt.block_on(project.get_feature_store())
}