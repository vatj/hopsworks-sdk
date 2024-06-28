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
trans_df = pl.read_csv(
    "https://repo.hops.works/master/hopsworks-tutorials/data/card_fraud_data/transactions.csv",
    try_parse_dates=True,
)
print(trans_df.head(5))

# %%
version = 1
local_fg = fs.get_or_create_feature_group(
    name="test_fg",
    version=version,
    description="test_fg",
    primary_key=["cc_num"],
    event_time="datetime",
    online_enabled=True,
)

# %%
try:
    print("Register the feature group if it does not exist")
    local_fg.save(trans_df)
    print("Feature group registered")
except Exception as e:
    print(e)

# %%
try:
    print("Insert a polars dataframe into the feature store")
    local_fg.insert(trans_df.head(10))
    print("polars_df inserted into Kafka")
except Exception as e:
    print(e)

# %%
try:
    print("Read from online feature store to arrow record batch")
    arrow_rb = local_fg.read_from_online_store(return_type="pyarrow")
    print("arrow_rb : \n", arrow_rb)
except Exception as e:
    print(e)


# %%
try:
    print("Get the record batch from the offline store")
    arrow_rb = local_fg.read_from_offline_store(return_type="pyarrow")
    print("arrow_rb : ", arrow_rb)
except Exception as e:
    print(e)

# %%
try:
    print("Get the polars dataframe from the offline store")
    polars_df = local_fg.read_from_online_store(return_type="polars")
    print("polars_df : ", polars_df.head(5))
    print(f"shape: {polars_df.shape}")
except Exception as e:
    print(e)

# %%
