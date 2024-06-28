# %%
from hopsworks_sdk import login
import pathlib
import os
import toml
import logging
import polars as pl

config_name = "managed-config.toml"
config_path = pathlib.Path(os.getcwd()) / "configs" / config_name
print(config_path)
config = toml.load(config_path)
print(config)

os.environ["HOPSWORKS_API_KEY"] = config["env"]["HOPSWORKS_API_KEY"]

if config["env"].get("RUST_LOG", None):
    FORMAT = "%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s"
    logging.basicConfig(format=FORMAT)
    logging.getLogger().setLevel(
        logging.DEBUG if config["env"]["RUST_LOG"] else logging.INFO
    )
    logging.info("RUST_LOG set to %s", config["env"]["RUST_LOG"])

project = login()
fs = project.get_feature_store()

# %%
existing_fg = fs.get_feature_group(config["env"].get("HOPSWORKS_FEATURE_GROUP_NAME", ""), 1)

if existing_fg is None:
    raise ValueError("Feature group does not exist, exiting...")

# %%

try:
    print("Get the record batch from the offline store")
    arrow_rb = existing_fg.read_from_offline_store(return_type="pyarrow")
    print("arrow_rb : ", arrow_rb)
except Exception as e:
    print(e)

# %%

try:
    print("Get the polars dataframe from the offline store")
    polars_df = existing_fg.read_from_offline_store(return_type="polars")
    print("polars_df : ", polars_df.head(5))
    print(f"shape: {polars_df.shape}")
except Exception as e:
    print(e)

# %%
# %%
polars_df = polars_df.with_columns(
    pl.lit(4.0).alias("trans_volume_mstd"),
    pl.col("datetime").dt.replace_time_zone(None).alias("datetime"),
    cc_num=pl.Series(range(0, polars_df.shape[0])),
)
print(polars_df.head(5))

# %%
# %%
try:
    print("Insert a polars dataframe into the feature store")
    existing_fg.insert(polars_df)
    print("polars_df inserted into Kafka")
except Exception as e:
    print(e)
