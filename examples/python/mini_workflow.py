# %%
import os
import pathlib

import polars as pl
import toml
from hopsworks_sdk import login


config_name = "ovh-config.toml"
config_path = pathlib.Path(os.getcwd()) / "configs" / config_name
print(config_path)
config = toml.load(config_path)
print(config)

os.environ["HOPSWORKS_API_KEY"] = config["env"]["HOPSWORKS_API_KEY"]

# %%
print("Login to your Hopsworks Cluster")

project = login(
    api_key_value=os.environ["HOPSWORKS_API_KEY"],
    url=config["env"]["HOPSWORKS_URL"],
    project_name=config["env"]["HOPSWORKS_PROJECT_NAME"],
)
fs = project.get_feature_store()

# %%
trans_df = pl.read_csv(
    "https://repo.hops.works/master/hopsworks-tutorials/data/card_fraud_data/transactions.csv",
    try_parse_dates=True,
)
print(trans_df.head(5))

# %%
print("Create new Feature Group: Inserting data using the Rust engine")
version = 1
simple_fg = fs.get_or_create_feature_group(
    name="simple_fg",
    version=version,
    description="simple_fg with transactions data",
    primary_key=["cc_num"],
    event_time="datetime",
    online_enabled=True,
)

simple_fg.save_using_polars_dataframe_schema(trans_df)
job_execution = simple_fg.insert(trans_df.head(10))

# %%
print(
    "Materializing Data to the Offline Feature Store is done asynchronously by a spark job running on the Hopsworks cluster."
)
print(f"Job execution metadata: {job_execution}")

# %%
print("Rust-backed Feature view from a subset of feature from a single Feature Group")
query = simple_fg.select(["cc_num", "amount", "datetime"])
simple_fv = fs.create_feature_view(
    name="simple_fv",
    query=query,
    version=version,
    description=f"Feature View based on features selected from {simple_fg.name}_v{simple_fg.version}",
)


# %%
print("Feature View: Reading data using the Rust engine")
simple_fv.init_online_store_rest_client(api_key=os.environ["HOPSWORKS_API_KEY"])
feature_vector = simple_fv.get_feature_vector(entry={"cc_num": 4817626088411704})


# %%
print("Cleaning up")
simple_fg.delete()
simple_fv.delete()
