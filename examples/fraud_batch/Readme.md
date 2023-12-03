# <span style="font-width:bold; font-size: 3rem; color:#1EB182;"><img src="../images/icon102.png" width="38px"></img> **Hopsworks Feature Store** </span><span style="font-width:bold; font-size: 3rem; color:#333;">Quick Start - Fraud Batch Tutorial</span>

> :warning: **Readme based on the official [fraud batch example](https://github.com/logicalclocks/hopsworks-tutorials/master/fraud_batch) from the [Hopsworks tutorial repository](https://github.com/logicalclocks/hopsworks-tutorials)**: Despite some modifications, it is possible that some of the information below do not apply to the Rust API!

<span style="font-width:bold; font-size: 1.4rem;"> This is a quick-start of the Hopsworks Feature Store; using a fraud use case you will load data into the feature store, create two feature groups from which we will make a training dataset. This is a <b>batch use case</b>, it will give you a high-level view of how to use our python APIs and the UI to navigate the feature groups, use them to create feature views and training datasets using Hopsworks Feature Store. </span>

## **🗒️ This tutorial is divided into the following parts:**

1. **Feature Pipeline**: How to load, engineer and create feature groups.
2. **Training Pipeline**: How to build a feature view and create a training dataset.

## Prerequisites

To run this tutorial, you need an account on Hopsworks. You can create a new account at [app.hopsworks.ai](https://app.hopsworks.ai).
Export the API key as environment variable in your terminal:

```bash
export HOPSWORKS_API_KEY=<your-api-key>
```

# <span style="font-width:bold; font-size: 3rem; color:#1EB182;">Hopsworks: Main Concepts</span>

You may refer to the concept documentation on [docs.hopsworks.ai](https://docs.hopsworks.ai/concepts/) for an extensive overview of all the concepts and abstractions in the feature store. Beware while the concept documentation is not intended to be API specific, but some functionalities or concept may not yet be implemented in the Rust client.
Below are the concepts covered in this quick-start;

## Feature Store

The [Feature Store](https://www.hopsworks.ai/feature-store) is a data management system that allows data scientists and data engineers to efficiently collaborate on projects.

An organization might have separate data pipelines for each and every model they train. In many cases, this results in duplicate work, as the pipelines typically share some preprocessing steps in common. Consequently, changes in some preprocessing steps would result in even more duplicate work, and potential inconsistencies between pipelines.

Moreover, once a model has been deployed we need to make sure that online data is processed in the same way as the training data. The Feature Store streamlines the data pipeline creation process by putting all the feature engineering logic of a project in the same place. The built-in version control enables them to work seamlessly with different versions of features and datasets.

Another advantage of having a feature store set up is that you can easily do "time travel" and recreate training data as it would have looked at some point in the past. This is very useful when, for example, benchmarking recommendation systems or assessing concept drift.

In short, a feature store enables data scientists to reuse features across different experiments and tasks and to recreate datasets from a given point in time.

In `hospworks-rs`, entities, methods and functions to interact with the feature store are located under `hopsworks-rs::feature_store`. It is analoguous to the `hsfs` library which serves as the Python interface to the feature store.

### Feature Group

A [Feature Group](https://docs.hopsworks.ai/latest/concepts/fs/feature_group/fg_overview/) is a collection of conceptually related features that typically originate from the same data source. It is really up to the user (perhaps a data scientist) to decide which features should be grouped together into a feature group, so the process is subjective. Apart from conceptual relatedness or stemming from the same data source, another way to think about a feature group might be to consider features that are collected at the same rate (e.g. hourly, daily, monthly) to belong to the same feature group.

A feature group is stored in the feature store and can exist in several versions. New data can be freely inserted or upserted to a feature group, as long as it conforms to its schema. To change the schema of a feature group, visit the [Hopsworks UI](https://app.hopsworks.ai).

### Data Validation

As of now there is no immediate plan to implement data validation in the Rust API. Hopsworks provides some integration with Great Expectations to support data validation in the python or spark API. However Great Expectations does not have a drop-in Rust equivalent, which means it is left as future work to investigate a suitable library.

### Feature Engineering

Typically it does not suffice to train a model using raw data sources alone. There can for instance be data problems such as missing values and duplicate entries that need to be dealt with. Moreover, model training could be facilitated by preprocessing raw features, or creating new features using raw features.

Feature engineering can be considered a whole subfield by itself and there are general resources such as [Feature Engineering and Selection: A Practical Approach for Predictive Models](https://www.amazon.com/Feature-Engineering-Selection-Practical-Predictive-dp-1138079227/dp/1138079227/ref=as_li_ss_tl?_encoding=UTF8&me=&qid=1588630415&linkCode=sl1&tag=inspiredalgor-20&linkId=f3f8d9f56031a030893aad8fc684a800&language=en_US). While it seems that Python as become the default language to write feature pipelines, various Rust libraries are starting to emerge. Checkout [Polars](https://github.com/pola-rs/polars) or [Apache Datafusion](https://github.com/apache/arrow-datafusion).

The Feature Store often serves as a sink to Feature Engineering pipelines. A generic pipeline would load raw data are from a datawarehouse, process and transform them as needed. The resulting dataframe would be inserted/upserted into a Feature Group to be stored in the Feature Store. Hopsworks supports both batch writing and continuous ingestion of data into feature groups via Kafka.

### Feature View and Training Dataset

The [Feature View](https://docs.hopsworks.ai/latest/concepts/fs/feature_view/fv_overview/) makes it easy to select Features from various Feature Groups to create training datasets adapted to your specific use case. A Feature View is a logical view of a set of features, and is defined by a query that selects and joins features from one or more feature groups. This base query serves to read data from the feature store and materialize a [Training Datasets](https://docs.hopsworks.ai/latest/concepts/fs/feature_view/offline_api/). This query is saved as metadata in the dataset, which makes it easy to see the dependencies between the dataset and the feature groups it originates from. The Hopsworks UI contains a dataset provenance graph that shows this.

In the Hopsworks framework, training datasets are immutable in the sense that, in contrast to feature groups, you cannot append data to them after they have been created. However, you _can_ have different _versions_ of your training data, based for example on the range of date you select in the feature view query. This is useful when you want to compare the performance of different models trained on different versions of the training data.
