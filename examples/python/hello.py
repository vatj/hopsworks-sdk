# %%
from hopsworks_sdk import login
import pathlib
import os
import toml
import logging

config_name = "managed-config.toml"
config_path = pathlib.Path(os.getcwd()) / "configs" / config_name
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
print(dir(fs))

fg = fs.get_feature_group("transactions_4h_aggs_fraud_batch_fg_5_rust", 1)

print(fg)
print(dir(fg))


try:
    print("Get the record batch from the offline store")
    arrow_rb = fg.read_arrow_from_offline_store()
    print("arrow_rb : ", arrow_rb)
except Exception as e:
    print(e)

try:
    print("Get the polars dataframe from the offline store")
    polars_df = fg.read_polars_from_offline_store()
    print("polars_df : ", polars_df)
except Exception as e:
    print(e)





# %%
