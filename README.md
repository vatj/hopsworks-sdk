# HOPWORKS-RS

Hopsworks-rs is a Rust SDK to interact with the Hopsworks Platform and Feature Store. It is intended to be used in conjunction with the [Hopsworks Feature Store](https://www.hopsworks.ai/the-ml-platform-for-batch-and-real-time-data) to build end-to-end machine learning pipelines. Using Rust and Hopsworks-rs you can put real-time data pipelines, feature engineering, model training and serving in the same system and leverage the power of the Feature Store to build and deploy AI/ML systems faster.

As of now the SDK is in early development and only supports a subset of Hopsworks capabilities. The public api should not be considered stable, as it is still unclear whether it will evolve to be more idiomatic Rust or stay closer to the Python SDK for simplicity. The aim is to kickstart a community project. As such contributions are welcome. For production use-case you can checkout the [python or java SDK](https://pypi.org/project/hopsworks/). Much of the implementation is based on the python SDK and can therefore sometimes feel a bit unidiomatic for Rust developers.

## Quickstart

To get started with minimal setup you can use [Hopsworks Serverless Platform](https://app.hopsworks.ai/) to register for a free account. Once you have registered you can create your project and follow the instructions to create an api key.

Copy the config-template.toml to config.toml in the `configs` directory and paste your api key and project name. Download some examples data:

```bash
chmod +x scripts/data/download_example_data.sh && ./scripts/data/download_example_data.sh
```

You are ready to run your first examples and start using Hopsworks Feature Store. First you can run the feature engineering example. It contains minimal logic to load some data, transform it and write it to a Feature Group. Feature Groups are the primary abstraction in the Feature Store and are used to store Feature Data, similar to tables in a relational database.

```bash
cargo run --example fraud_batch/feature_engineering_pipeline --config configs/config.toml
```

Once your data is ingested in the Feature Store, you can familiarize with Feature View. Feature Views allow you to select features from different Feature Groups and help you join them together. Pulling Feature data from various sources allows you to tailor a Feature View to a particular use case i.e ML/AI model. Feature View can read api is specifically designed to materialize data from Feature Groups into training datasets for ML/AI models.

```bash
cargo run --example fraud_batch/training_pipeline --config configs/config.toml
```

As the primary read interface of the Feature Store, Feature Views allow you to both materialize training datasets but also serve Feature vector for batch or real-time ML-systems. Only in the python and java SDKs for now...

## Contributing

Contributions are welcome, although the contributing.md is still a work in progress. If you have any questions, suggestions or want to contribute, feel free to open a github issue for now.

## Open-Source

Hopsworks-rs is available under the AGPL-V3 license. In plain English this means that you are free to use it and even build paid services on it, but if you modify the source code, you should also release back your changes and any systems built around it as AGPL-V3.
