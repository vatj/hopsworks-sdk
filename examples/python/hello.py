# %%
from hopsworks_sdk import login
import pathlib
import os
import toml
import logging
import polars as pl

config_name = "managed-config.toml"
config_path = pathlib.Path(os.getcwd()) / "../../configs" / config_name
print(config_path)
config = toml.load(config_path)
print(config)

os.environ["HOPSWORKS_API_KEY"] = config["env"]["HOPSWORKS_API_KEY"]

if config["env"].get("RUST_LOG", None):
    FORMAT = '%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s'
    logging.basicConfig(format=FORMAT)
    logging.getLogger().setLevel(logging.DEBUG if config["env"]["RUST_LOG"] else logging.INFO)
    logging.info("RUST_LOG set to %s", config["env"]["RUST_LOG"])

project = login()
fs = project.get_feature_store()

print(fs)
print([method for method in dir(fs) if not method.startswith("_")])

fg = fs.get_feature_group("transactions_4h_aggs_fraud_batch_fg_5_rust", 1)

print(fg)
print([method for method in dir(fg) if not method.startswith("_")])

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
    local_fg.register_feature_group(trans_df)
    print("Feature group registered")
except Exception as e:
    print(e)

# %%
try:
    print("Insert a polars dataframe into the feature store")
    local_fg.insert_polars_df_into_kafka(trans_df.head(10))
    print("polars_df inserted into Kafka")
except Exception as e:
    print(e)

# %%

try:
    print("Get the record batch from the offline store")
    arrow_rb = fg.read_arrow_from_offline_store()
    print("arrow_rb : ", arrow_rb)
except Exception as e:
    print(e)

# %%

try:
    print("Get the polars dataframe from the offline store")
    polars_df = fg.read_polars_from_offline_store()
    print("polars_df : ", polars_df.head(5))
    print(f"shape: {polars_df.shape}")
except Exception as e:
    print(e)

# %%
polars_df = polars_df.with_columns(
    pl.lit(4.0).alias("trans_volume_mstd"),
    pl.col("datetime").dt.replace_time_zone(None).alias("datetime"),
    cc_num=pl.Series(range(0, polars_df.shape[0])),
)

print(polars_df.head(5))

# %%

try:
    print("Insert a polars dataframe into the feature store")
    fg.insert_polars_df_into_kafka(polars_df)
    print("polars_df inserted into Kafka")
except Exception as e:
    print(e)


# %%
try:
    print("Read from online feature store to arrow record batch")
    arrow_rb = local_fg.read_arrow_from_sql_online_store()
    print("arrow_rb : \n", arrow_rb)
except Exception as e:
    print(e)


# %%
try:
    print("Get the record batch from the offline store")
    arrow_rb = local_fg.read_arrow_from_offline_store()
    print("arrow_rb : ", arrow_rb)
except Exception as e:
    print(e)

# %%
try:
    print("Get the polars dataframe from the offline store")
    polars_df = local_fg.read_polars_from_sql_online_store()
    print("polars_df : ", polars_df.head(5))
    print(f"shape: {polars_df.shape}")
except Exception as e:
    print(e)

# %%
