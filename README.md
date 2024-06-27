# Hopsworks SDK

Disclaimer: This project is not an official client for the Hopsworks Platform and its Feature Store, and is not maintained by the Hopsworks team. It is rather a proof of concept and a personal project to explore the capabilities of Rust in supporting end-to-end machine learning pipelines.

[Hopsworks Feature Store](https://www.hopsworks.ai/the-ml-platform-for-batch-and-real-time-data) has both production-ready [python or java SDK](https://pypi.org/project/hopsworks/) to support a wide variety of use cases. This project aims to provide a *Rust SDK* as well as corresponding *python bindings* to interact with the Hopsworks platform and its Feature Store. As of now the SDK is in early development and only supports a subset of Hopsworks capabilities. The public api should not be considered stable, as it is still unclear whether it will evolve to be more idiomatic Rust or stay closer to the Python SDK for simplicity.

Currently, this project is driven by a single person. However there is a lot more to do than what I have time for. As such contributions are welcome. Please reach out or open an issue. This repository also contains some CLI tools to interact with the Hopsworks platform. Again these are hobby projects and receive a corresponding amount of attention and energy.

## Quickstart

### Step 1: Register for Hopsworks Serverless Platform

If you have your own Hopsworks cluster check out [this section](#connect-to-your-own-hopsworks-cluster).

To get started with minimal setup you can use [Hopsworks Serverless Platform](https://app.hopsworks.ai/) to register for a free account. Once you have registered you can create your project and follow the instructions to create an api key. Save it for later! From there you can head to the examples directory which has a few tutorials or follow the quickstart below to get a feel for hopsworks SDK.

### Step 2: Install dependencies, build and compile the SDK

As of now there is no released binaries for this library, you need to compile it yourself.

Get the [rye package manager](https://rye.astral.sh/guide/installation/):

```bash
curl -sSf https://rye.astral.sh/get | bash # Checkout the website for a more secure way to install
git clone https://github.com/vatj/hopsworks-sdk
cd hopsworks-sdk
rye sync
source .venv/bin/activate
cp configs/config-template.toml configs/managed-config.toml
```

Copy your API key in the newly created config file and set the correct project name.

### Step 3: Start playing around

Look at the hello.py example in the `examples/python` directory to get started.

## Benefit of using Rust

There are several benefits to making mixed rust/python modules:
- Performance critical part can be written in Rust and called from Python
- Dependencies on some python packages can be avoided by hiding them in the rust binaries
- New compute engines are being written in Rust, it will be more efficient to interface using Rust to enable performance critical code path to be efficient
- Rust Arrow implementation is extremely complete and actively developed

## Contributing

Contributions are welcome, although the contributing.md is still a work in progress. If you have any questions, suggestions or want to contribute, feel free to open a github issue for now.

## Open-Source

Hopsworks-rs is available under the AGPL-V3 license. In plain English this means that you are free to use it and even build paid services on it, but if you modify the source code, you should also release back your changes and any systems built around it as AGPL-V3.
