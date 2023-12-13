# Examples and Tutorials

Work in progress to provide a Rust version of some of the examples and tutorials from the official [Hopsworks repository](https://github.com/logicalclocks/hopsworks-tutorials). Numerous functionalities are either not implemented or implemented differently in the Rust API. Therefore the examples are neither an exact match nor fully functional yet. Contributions to port parts of the examples are welcome!

Some examples are also simply snippets that demonstrate how to use a specific functionality. They are somewhat redundant with either unit or doc tests, but included as they can be helpful template scripts. You can find them in the `snippets` folder.

## Quickstart

### Step 1: Register for Hopsworks Serverless Platform

To get started with minimal setup you can use [Hopsworks Serverless Platform](https://app.hopsworks.ai/) to register for a free account. Once you have registered you can create your project and follow the instructions to create an api key. Save it for later.

### Step 2: Run the fraud batch tutorials locally with your registered SDK

Copy the config-template.toml to config.toml in the `configs` directory and paste your api key and project name. Download some examples data:

```bash
chmod +x scripts/data/download_example_data.sh && ./scripts/data/download_example_data.sh
```

You are ready to run your first examples and start using Hopsworks Feature Store. First you can run the feature engineering example. It contains minimal logic to load some data, transform it and write it to a Feature Group. Feature Groups are the primary abstraction in the Feature Store and are used to store Feature Data, similar to tables in a relational database.

```bash
cargo run --example fraud_batch_feature_pipeline --config configs/config.toml
```

Once your data is ingested in the Feature Store, you can familiarize with Feature View. Feature Views allow you to select features from different Feature Groups and help you join them together. Pulling Feature data from various sources allows you to tailor a Feature View to a particular use case i.e ML/AI model. Feature View can read api is specifically designed to materialize data from Feature Groups into training datasets for ML/AI models.

```bash
cargo run --example fraud_batch_training_pipeline --config configs/config.toml
```

As the primary read interface of the Feature Store, Feature Views allow you to both materialize training datasets but also serve Feature vector for batch or real-time ML-systems. Only in the python and java SDKs for now...
