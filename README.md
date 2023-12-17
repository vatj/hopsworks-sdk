# hopsworks_rs

Hopsworks-rs is a Rust SDK to interact with the Hopsworks Platform and Feature Store. It is intended to be used in conjunction with the [Hopsworks Feature Store](https://www.hopsworks.ai/the-ml-platform-for-batch-and-real-time-data) to build end-to-end machine learning pipelines. Using Rust and Hopsworks-rs you can put real-time data pipelines, feature engineering, model training and serving in the same system and leverage the power of the Feature Store to build and deploy AI/ML systems faster.

As of now the SDK is in early development and only supports a subset of Hopsworks capabilities. The public api should not be considered stable, as it is still unclear whether it will evolve to be more idiomatic Rust or stay closer to the Python SDK for simplicity. The aim is to kickstart a community project. As such contributions are welcome. For production use-case you can checkout the [python or java SDK](https://pypi.org/project/hopsworks/). Much of the implementation is based on the python SDK and can therefore sometimes feel a bit unidiomatic for Rust developers.

## Quickstart

### Step 1: Register for Hopsworks Serverless Platform

If you have your own Hopsworks cluster check out this section.

To get started with minimal setup you can use [Hopsworks Serverless Platform](https://app.hopsworks.ai/) to register for a free account. Once you have registered you can create your project and follow the instructions to create an api key. Save it for later! From there you can head to the examples directory which has a few tutorials or follow the quickstart below to get a feel for hopsworks SDK.

### Step 2: Connect to your project and start writing data to the Feature Store

```rust
use color_eyre::Result;
use polars::prelude::*;
use hopsworks_rs::hopsworks_login;

#[tokio::main]
async fn main() -> Result<()> {
 // The api key will be read from the environment variable HOPSWORKS_API_KEY
 let project = hopsworks_login(None).await?;
 // Get the default feature store for the project
 let fs = project.get_feature_store().await?;


 // Read data from a local csv file into a Polars DataFrame
 let mut df = CsvReader::from_path("./examples/data/transactions.csv")?.finish()?;

 // Create a new feature group and ingest local data to the Feature Store
 let fg = fs.create_feature_group(
   "my_fg",
   1,
   None,
   vec!["primary_key_feature_name(s)"],
   Some("event_time_feature_name"),
   false
 )?;
 fg.insert(&mut df).await?;

 Ok(())
}
```

And that's it! You have now created a Feature Group (FG) and ingested data into it. Build a query by selecting and joining features from different FGs to make a Feature View. They allow you to read data from the Feature Store directly into training datasets which allow you to start experimenting with your ML/AI models right on.

```rust
use color_eyre::Result;
use hopsworks_rs::hopsworks_login;

#[tokio::main]
async fn main() -> Result<()> {
  // The api key will be read from the environment variable HOPSWORKS_API_KEY
  let project = hopsworks_login(None).await?;
  // Get the default feature store for the project
  let fs = project.get_feature_store().await?;

  // Get two feature groups
  let fg1 = fs.get_feature_group("my_fg1").await?.expect("Feature group not found");
  let fg2 = fs.get_feature_group("my_fg2").await?.expect("Feature group not found");

  // Select and join features
  let query = fg1.select(&["feature1", "feature2"])
    .join(&fg2.select(&["feature3", "feature4"]), None)
    .await?;

  // Create your Feature View
  let fv = fs.create_feature_view(
    "my_fv",
    1,
    &query,
    None,
  );

  // Read data from the Feature Store into an in memory DataFrame
  let mut df = fv.read_with_arrow_flight_client().await?;

  // Do some ML/AI stuff with the data
  Ok(())
}
```

### Connect to your own Hopsworks cluster

If you have your own Hopsworks cluster you can use the SDK to connect to it. You will need your api key and hopsworks domain name. Simply provide a HopsworksClientBuilder to the login function.

```rust
use color_eyre::Result;
use hopsworks_rs::{hopsworks_login, HopsworksClientBuilder};

#[tokio::main]
async fn main() -> Result<()> {
    // The api key will be read from the environment variable HOPSWORKS_API_KEY
  let client_builder = HopsworksClientBuilder::default()
                        .with_domain("www.my.hopsworks.domain.com");
  let project = hopsworks_login(Some(client_builder)).await?;
  // Get the default feature store for the project
  let fs = project.get_feature_store().await?;

  // Check out the examples directory or the quickstart
  Ok(())
}
```

## Contributing

Contributions are welcome, although the contributing.md is still a work in progress. If you have any questions, suggestions or want to contribute, feel free to open a github issue for now.

## Open-Source

Hopsworks-rs is available under the AGPL-V3 license. In plain English this means that you are free to use it and even build paid services on it, but if you modify the source code, you should also release back your changes and any systems built around it as AGPL-V3.
