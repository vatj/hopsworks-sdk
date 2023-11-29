# HOPWORKS-RS

Hopsworks-rs is a Rust SDK to interact with the Hopsworks Platform and Feature Store. It is intended to be used in conjunction with the [Hopsworks Feature Store](https://www.hopsworks.ai/the-ml-platform-for-batch-and-real-time-data) to build end-to-end machine learning pipelines. Hopsworks-rs allows you to leverage the power of Rust to build and deploy resilient high-performance AI/ML systems.

As of now the SDK is in early development and only supports a subset of Hopsworks capabilities. It is a community project and contributions are welcome. For production use-case you can checkout the [python or java SDK](https://pypi.org/project/hopsworks/). Much of the implementation is based on the python SDK and can therefore sometimes feel a bit unidiomatic for Rust developers.

## Quickstart

To get started with minimal setup you can use [Hopsworks Serverless Platform](https://app.hopsworks.ai/) to register for a free account. Once you have registered you can create your project and follow the instructions to create an api key.

Copy the config-template.toml to config.toml in the `configs` directory and paste your api key and project name. You can create your first Feature Groups and populate them with some data by runnning:

```bash
cargo run --example fraud_batch/feature_engineering_pipeline --config configs/config.toml
```

Once you have inserted data in the feature store, you can use Feature View to read from it. Feature Views allow you to select features from different Feature Groups and help you join them together. They provide a unified view of your data and can be used to train models or serve feature vectors for inference. To create a Feature View:

```bash
cargo run --example fraud_batch/training_pipeline --config configs/config.toml
```

## Contributing

Contributions are welcome, although the contributing.md is still a work in progress. If you have any questions, suggestions or want to contribute, feel free to open a github issue for now.

## Open-Source

Hopsworks-rs is available under the AGPL-V3 license. In plain English this means that you are free to use it and even build paid services on it, but if you modify the source code, you should also release back your changes and any systems built around it as AGPL-V3.
