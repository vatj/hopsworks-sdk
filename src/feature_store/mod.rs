//! [`FeatureStore`] client to write, read and manage Feature data.
pub mod feature_group;
pub mod feature_view;
pub mod query;

pub use feature_group::FeatureGroup;
pub use feature_view::FeatureView;

use color_eyre::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{
    core::feature_store::{
        feature_group::get_feature_group_by_name_and_version,
        feature_view::{create_feature_view, get_feature_view_by_name_and_version},
        training_dataset::get_training_dataset_by_name_and_version,
        transformation_function::get_transformation_function_by_name_and_version,
    },
    repositories::feature_store::entities::FeatureStoreDTO,
};

use self::{
    feature_view::{
        training_dataset::TrainingDataset, transformation_function::TransformationFunction,
    },
    query::Query,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FeatureStore {
    // The [`FeatureStore`] lies at the heart of the Hopsworks MLOps platform.
    // It is a centralized repository of Feature data that can be used both for training
    // and serving machine learning models. [`FeatureGroup`]
    // is the perfect sink for all Feature Engineering pipelines, allowing you to easily share Features across teams and projects.
    // [`FeatureView`]s allow you to group these Features
    // to serve as schema for a ML model and a convenient interface to read data from the [`FeatureStore`].
    // They provide methods to read or materialize on-disk training datasets, serve Features in real-time and
    // define transformations to apply to the raw data before serving it to the model.
    //
    //
    // # Examples
    //
    // ### Create a Feature Group and insert a Polars DataFrame
    // ```no_run
    // # use color_eyre::Result;
    // # use hopsworks_rs::hopsworks_login;
    // # use polars::prelude::*;
    //
    // # async fn run() -> Result<()> {
    //    // The api key will be read from the environment variable HOPSWORKS_API_KEY
    //    let fs = hopsworks_login(None).await?.get_feature_store().await?;
    //
    //    // Create a new feature group
    //    let fg = fs.create_feature_group(
    //       "my_fg",
    //       1,
    //       None,
    //       vec!["primary_key_feature_name(s)"],
    //       Some("event_time_feature_name"),
    //       false
    //    )?;
    //
    //    // Ingest data from a CSV file
    //    let mut df = CsvReader::from_path("./examples/data/transactions.csv")?.finish()?;
    //
    //    // Insert data into the feature group
    //    fg.insert(&mut df).await?;
    // #   Ok(())
    // # }
    // ```
    //
    // ### Create a Feature View to read data from Feature belonging to different Feature Groups
    // ```no_run
    // # use color_eyre::Result;
    // # use hopsworks_rs::hopsworks_login;
    // # use polars::prelude::*;
    //
    // # async fn run() -> Result<()> {
    //   // The api key will be read from the environment variable HOPSWORKS_API_KEY
    //   let fs = hopsworks_login(None).await?.get_feature_store().await?;
    //
    //  // Get Feature Groups by name and version
    //  let fg1 = fs.get_feature_group("fg1", Some(1)).await?.expect("Feature Group not found");
    //  let fg2 = fs.get_feature_group("fg2", Some(1)).await?.expect("Feature Group not found");
    //
    //  // Create a Feature View
    //  let query = fg1.select(vec!["feature1", "feature2"])?
    //     .join(fg2.select(vec!["feature3"])?, None);
    //  let feature_view = fs.create_feature_view("my_feature_view", 1, query, None).await?;
    //
    //  // Read data from the Feature View
    //  let df = feature_view.read_with_arrow_flight_client().await?;
    // #  Ok(())
    // # }
    // ```
    //
    // ### Create a Training Dataset
    // ```no_run
    // # use color_eyre::Result;
    // # use hopsworks_rs::hopsworks_login;
    //
    // # async fn run() -> Result<()> {
    //  // The api key will be read from the environment variable HOPSWORKS_API_KEY
    //  let fs = hopsworks_login(None).await?.get_feature_store().await?;
    //
    //  // Get Feature View
    //  let feature_view = fs.get_feature_view("my_feature_view", Some(1))
    //     .await?.expect("Feature View not found");
    //
    //  // Create a Training Dataset
    //  let td = feature_view.create_attached_training_dataset().await?;
    // # Ok(())
    // # }
    // ```
    num_feature_groups: i32,
    num_training_datasets: i32,
    num_storage_connectors: i32,
    num_feature_views: i32,
    pub featurestore_id: i32,
    pub featurestore_name: String,
    created: String,
    project_name: String,
    project_id: i32,
    featurestore_description: Option<String>,
    online_featurestore_name: String,
    online_featurestore_size: Option<f64>,
    online_enabled: bool,
}

impl FeatureStore {
    pub fn new_from_dto(feature_store_dto: FeatureStoreDTO) -> Self {
        Self {
            num_feature_groups: feature_store_dto.num_feature_groups,
            num_training_datasets: feature_store_dto.num_training_datasets,
            num_storage_connectors: feature_store_dto.num_storage_connectors,
            num_feature_views: feature_store_dto.num_feature_views,
            featurestore_id: feature_store_dto.featurestore_id,
            featurestore_name: feature_store_dto.featurestore_name,
            created: feature_store_dto.created,
            project_name: feature_store_dto.project_name,
            project_id: feature_store_dto.project_id,
            featurestore_description: feature_store_dto.featurestore_description,
            online_featurestore_name: feature_store_dto.online_featurestore_name,
            online_featurestore_size: feature_store_dto.online_featurestore_size,
            online_enabled: feature_store_dto.online_enabled,
        }
    }
}

impl From<FeatureStoreDTO> for FeatureStore {
    fn from(feature_store_dto: FeatureStoreDTO) -> Self {
        FeatureStore::new_from_dto(feature_store_dto)
    }
}

impl FeatureStore {
    /// Get a [`FeatureGroup`] by name and optional version. If no version is provided, the latest version is returned.
    /// Returns `None` if no [`FeatureGroup`] with the given name and version exists. [`FeatureGroup`]s are the main interface
    /// to insert or upsert Feature data to the Feature Store.
    ///
    /// # Arguments
    /// * `name` - The name of the [`FeatureGroup`]
    /// * `version` - The version of the [`FeatureGroup`]. If no version is provided, the latest version is returned.
    ///
    /// # Examples
    /// ```no_run
    /// # use color_eyre::Result;
    /// use hopsworks_rs::hopsworks_login;
    /// use polars::prelude::*;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   let feature_store = hopsworks_login(None).await?.get_feature_store().await?;
    ///   let feature_group = feature_store.get_feature_group("my_fg", Some(1)).await?.expect("Feature Group not found");
    ///
    ///   let mut df = CsvReader::from_path("./examples/data/transactions.csv")?.finish()?;
    ///   feature_group.insert(&mut df).await?;
    ///
    ///   Ok(())
    /// }
    /// ```
    pub async fn get_feature_group(
        &self,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<FeatureGroup>> {
        if let Some(feature_group_dto) =
            get_feature_group_by_name_and_version(self.featurestore_id, name, version).await?
        {
            Ok(Some(FeatureGroup::from(feature_group_dto)))
        } else {
            Ok(None)
        }
    }

    /// Get a [`FeatureGroup`] by name and optional version. If no version is provided, the latest version is returned.
    /// If the [`FeatureGroup`] does not exist in the backend, a local [`FeatureGroup`] entity is created.
    /// [`FeatureGroup`]s are the main interface to insert or upsert Feature data to the [`FeatureStore`].
    /// Convenience method to avoid needing a separate script for the first iteration of a Feature Engineering pipeline.
    ///
    /// # Arguments
    /// * `name` - The name of the [`FeatureGroup`]
    /// * `version` - The version of the [`FeatureGroup`]. If no version is provided, the latest version is returned.
    /// * `description` - Optional description of the [`FeatureGroup`]
    /// * `primary_key` - List of primary key(s) of the [`FeatureGroup`]
    /// * `event_time` - Optional event time of the [`FeatureGroup`]
    /// * `online_enabled` - Whether the [`FeatureGroup`] is online enabled or not
    ///
    /// # Examples
    /// ```no_run
    /// # use color_eyre::Result;
    /// use hopsworks_rs::hopsworks_login;
    /// use polars::prelude::*;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   let feature_store = hopsworks_login(None).await?.get_feature_store().await?;
    ///   let mut df = CsvReader::from_path("./examples/data/transactions.csv")?.finish()?;
    ///
    ///   let feature_group = feature_store.get_or_create_feature_group(
    ///     "my_fg",
    ///     Some(1),
    ///     None,
    ///     vec!["primary_key_feature_name(s)"],
    ///     Some("event_time_feature_name"),
    ///     false
    ///   ).await?;
    ///
    ///   feature_group.insert(&mut df).await?;
    ///
    ///   Ok(())
    /// }
    /// ```
    pub async fn get_or_create_feature_group(
        &self,
        name: &str,
        version: Option<i32>,
        description: Option<&str>,
        primary_key: Vec<&str>,
        event_time: Option<&str>,
        online_enabled: bool,
    ) -> Result<FeatureGroup> {
        if let Some(feature_group) = self.get_feature_group(name, version).await? {
            return Ok(feature_group);
        }

        // If FG does not exist in backend, create a local Feature Group entity not registered with Hopsworks
        self.create_feature_group(
            name,
            version.unwrap_or(1),
            description,
            primary_key,
            event_time,
            online_enabled,
        )
    }

    /// Create a [`FeatureGroup`] with the given name and version. The [`FeatureGroup`] is not registered with Hopsworks,
    /// until the first insert/upsert is performed.
    ///
    /// A [`FeatureGroup`] is the main interface to insert or upsert Feature data to the Feature Store.
    /// It is a logical representation of a Feature table in the Feature Store.
    /// A [`FeatureGroup`] is defined by a set of Features, primary key(s) and an optional event time.
    /// Additionally, a [`FeatureGroup`] can be configured to be online enabled, which means that the data is also
    /// available for real-time serving.
    ///
    /// > **Note**: Type of the Features are inferred from the data type of the columns in the DataFrame on first insertion,
    /// > setting the table schema for future inserts/upserts.
    ///
    /// # Arguments
    /// * `name` - The name of the [`FeatureGroup`]
    /// * `version` - The version of the [`FeatureGroup`]
    /// * `description` - Optional description of the [`FeatureGroup`]
    /// * `primary_key` - List of primary key(s) of the [`FeatureGroup`]
    /// * `event_time` - Optional event time of the [`FeatureGroup`]
    /// * `online_enabled` - Whether the [`FeatureGroup`] is online enabled or not
    ///
    /// # Examples
    /// ```no_run
    /// # use color_eyre::Result;
    /// use hopsworks_rs::hopsworks_login;
    /// use polars::prelude::*;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   let feature_store = hopsworks_login(None).await?.get_feature_store().await?;
    ///   let mut df = CsvReader::from_path("./examples/data/transactions.csv")?.finish()?;
    ///
    ///   let feature_group = feature_store.create_feature_group(
    ///     "my_fg",
    ///     1,
    ///     None,
    ///     vec!["primary_key_feature_name(s)"],
    ///     Some("event_time_feature_name"),
    ///     false
    ///   )?;
    ///  
    ///   feature_group.insert(&mut df).await?;
    ///
    ///   Ok(())
    /// }
    /// ```
    pub fn create_feature_group(
        &self,
        name: &str,
        version: i32,
        description: Option<&str>,
        primary_key: Vec<&str>,
        event_time: Option<&str>,
        online_enabled: bool,
    ) -> Result<FeatureGroup> {
        Ok(FeatureGroup::new_local(
            self,
            name,
            version,
            description,
            primary_key,
            event_time,
            online_enabled,
        ))
    }

    /// Create a [`FeatureView`] with the given name and version. The [`FeatureView`] is the main interface to read data from the Feature Store,
    /// either online for real-time or offline for batch applications. It is a logical view on top of one or more [`FeatureGroup`]s.
    /// The [`FeatureView`] is defined by a [`Query`] that selects Features from one or more [`FeatureGroup`]s. Query support joins, aggregations,
    /// filtering and transformations. The [`FeatureView`] also defines a set of transformations to apply to the raw data before serving it to the model.
    ///
    /// > **Note**: Applying transformation function to feature vector or dataframes is only supported in the
    /// > [Feature Store Python SDK](https://github.com/logicalclocks/feature-store-api). However you can still use the
    /// > [`TransformationFunction`] when registering a new [`FeatureView`] with the [`FeatureStore`] using the Rust SDK.
    ///
    /// # Arguments
    /// * `name` - The name of the [`FeatureView`]
    /// * `version` - The version of the [`FeatureView`]
    /// * `query` - The [`Query`] that defines the [`FeatureView`]
    /// * `transformation_functions` - Optional hashmap mapping a feature name to a [`TransformationFunction`]s registered in the [`FeatureStore`].
    ///
    /// # Examples
    /// ```no_run
    /// # use color_eyre::Result;
    /// use hopsworks_rs::hopsworks_login;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   // The api key will be read from the environment variable HOPSWORKS_API_KEY
    ///   let feature_store = hopsworks_login(None).await?.get_feature_store().await?;
    ///   let feature_group = feature_store.get_feature_group("my_fg", None).await?.expect("Feature Group not found");
    ///
    ///   let query = feature_group.select(vec!["feature1", "feature2"])?;
    ///   let feature_view = feature_store.create_feature_view(
    ///     "my_feature_view",
    ///     1,
    ///     query,
    ///     None
    ///   ).await?;
    ///
    ///   let df = feature_view.read_with_arrow_flight_client().await?;
    ///
    ///   Ok(())
    /// }
    /// ```
    pub async fn create_feature_view(
        &self,
        name: &str,
        version: i32,
        query: Query,
        transformation_functions: Option<HashMap<String, TransformationFunction>>,
    ) -> Result<FeatureView> {
        create_feature_view(
            self.featurestore_id,
            self.featurestore_name.clone(),
            name.to_owned(),
            version,
            query,
            transformation_functions,
        )
        .await
    }

    /// Get a [`FeatureView`] by name and optional version. If no version is provided, the latest version is returned.
    /// Returns `None` if no [`FeatureView`] with the given name and version exists. [`FeatureView`]s are the main interface
    /// to read data from the Feature Store, either online for real-time or offline for batch applications.
    ///
    /// # Arguments
    /// * `name` - The name of the [`FeatureView`]
    /// * `version` - The version of the [`FeatureView`]. If no version is provided, the latest version is returned.
    ///
    /// # Examples
    /// ```no_run
    /// # use color_eyre::Result;
    /// use hopsworks_rs::hopsworks_login;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   // The api key will be read from the environment variable HOPSWORKS_API_KEY
    ///   let feature_store = hopsworks_login(None).await?.get_feature_store().await?;
    ///   let feature_view = feature_store.get_feature_view("my_feature_view", Some(1)).await?.expect("Feature View not found");
    ///
    ///   let my_df = feature_view.read_with_arrow_flight_client().await?;
    ///
    ///   Ok(())
    /// }
    /// ```
    pub async fn get_feature_view(
        &self,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<FeatureView>> {
        get_feature_view_by_name_and_version(self.featurestore_id, name, version).await
    }

    /// Get a [`TransformationFunction`] by name and optional version. If no version is provided, the latest version is returned.
    /// Returns `None` if no [`TransformationFunction`] with the given name and version exists. However you can still use the
    /// [`TransformationFunction`] when registering a new [`FeatureView`] with the [`FeatureStore`].
    ///
    /// > **Note**: Applying transformation function to feature vector or dataframes is only supported in the
    /// > [Feature Store Python SDK](https://github.com/logicalclocks/feature-store-api).
    ///
    /// # Arguments
    ///  * `name` - The name of the [`TransformationFunction`]
    ///  * `version` - The version of the [`TransformationFunction`]. If no version is provided, the latest version is returned.
    ///
    /// # Examples
    /// ```no_run
    /// # use color_eyre::Result;
    /// use hopsworks_rs::hopsworks_login;
    /// use std::collections::HashMap;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   // The api key will be read from the environment variable HOPSWORKS_API_KEY
    ///   let feature_store = hopsworks_login(None).await?.get_feature_store().await?;
    ///   let feature_group = feature_store.get_feature_group("my_fg", None).await?.expect("Feature Group not found");
    ///
    ///   let query = feature_group.select(vec!["feature1", "feature2"])?;
    ///   let transformation_function = feature_store.get_transformation_function("min_max_scaler", Some(1)).await?;
    ///   let transformation_functions = HashMap::from([("feature1".to_owned(), transformation_function.unwrap())]);
    ///   
    ///   let feature_view = feature_store.create_feature_view(
    ///     "my_feature_view",
    ///     1,
    ///     query,
    ///     Some(transformation_functions),
    ///   ).await?;
    ///
    ///   Ok(())
    /// }
    /// ```
    pub async fn get_transformation_function(
        &self,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<TransformationFunction>> {
        get_transformation_function_by_name_and_version(self.featurestore_id, name, version).await
    }

    /// Get a [`TrainingDataset`] by name and optional version. If no version is provided, the latest version is returned.
    /// Returns `None` if no [`TrainingDataset`] with the given name and version exists.
    /// Note that this method does not return the actual data of the [`TrainingDataset`], but only the metadata objects.
    /// Depending on the type of [`TrainingDataset`], you can either read the data in-memory directly or download the corresponding file from Hopsworks.
    ///
    /// # Arguments
    ///   * `name` - The name of the [`TrainingDataset`]
    ///   * `version` - The version of the [`TrainingDataset`]. If no version is provided, the latest version is returned.
    ///
    /// # Examples
    /// ```no_run
    /// # use color_eyre::Result;
    /// use hopsworks_rs::hopsworks_login;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///   // The api key will be read from the environment variable HOPSWORKS_API_KEY
    ///   let feature_store = hopsworks_login(None).await?.get_feature_store().await?;
    ///   let td = feature_store.get_training_dataset("my_td", None).await?;
    ///
    ///   // TODO: Update when more methods are implemented on TrainingDataset
    ///
    ///   Ok(())
    /// }
    /// ```
    pub async fn get_training_dataset(
        &self,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<TrainingDataset>> {
        get_training_dataset_by_name_and_version(self.featurestore_id, name, version).await
    }
}
