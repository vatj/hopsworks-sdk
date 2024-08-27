# %%
from hopsworks_sdk import login
from hopsworks_sdk.hopsworks_rs import init_subscriber
import pathlib
import os
import toml
import logging
import polars as pl

config_name = "ovh-config.toml"
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

init_subscriber()
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
    print("Create a query from the features of a single Feature Group")
    query = local_fg.select(["cc_num", "amount", "datetime"])
    print("Query created")
except Exception as e:
    print(e)

# %%
try:
    print("Create a feature view object from the query")
    simple_fv = fs.create_feature_view(
        name="simple_fv",
        query=query,
        version=1,
        description=f"Feature View based on features selected from {local_fg.name}_v{local_fg.version}",
    )
    print(f"Feature view created: {simple_fv.name}_v{simple_fv.version}")
except Exception as e:
    print(e)

# %%
try:
    print("Init Online Store Rest Client")
    simple_fv.init_online_store_rest_client(api_key=os.environ["HOPSWORKS_API_KEY"])
    print("Online Store Rest Client initialized")
except Exception as e:
    print(e)

# %%
try:
    print("Fetch real-time feature values for a given primary key entry")
    feature_vector = simple_fv.get_feature_vector(entries={"cc_num": 4817626088411704})
    print("Got feature vector: ", feature_vector)
except Exception as e:
    print(e)


# %%
print(f"Delete feature group {local_fg.name}_v{local_fg.version}")
local_fg.delete()
print("Feature group deleted")

# %%
print(f"Delete feature view {simple_fv.name}_v{simple_fv.version}")
simple_fv.delete()
print("Feature view deleted")