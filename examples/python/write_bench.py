# %%
from hopsworks_sdk import login
import pathlib
import os
import toml
import time
import logging
import random
import polars as pl

config_name = "managed-config.toml"
config_path = pathlib.Path(os.getcwd()) / "configs" / config_name
print(config_path)
config = toml.load(config_path)
print(config)

os.environ["HOPSWORKS_API_KEY"] = config["env"]["HOPSWORKS_API_KEY"]

project = login()
fs = project.get_feature_store()

# %%
taxi_df = pl.read_csv(
    "https://repo.hops.works/dev/davit/nyc_taxi/rides500.csv",
    try_parse_dates=True,
)
print(taxi_df.head(5))
print(taxi_df.shape)

# # %%

# name = "".join(random.choices(["a", "b", "c", "d", "e", "f", "g", "h"], k=5))

# version = 1
# local_fg = fs.get_or_create_feature_group(
#     name=name,
#     version=version,
#     description="benchmark_fg",
#     primary_key=["cc_num"],
#     event_time="datetime",
#     online_enabled=False,
# )

# # %%
# try:
#     print("Register the feature group if it does not exist")
#     local_fg.save(trans_df)
#     print("Feature group registered")
# except Exception as e:
#     print(e)

# # %%
# try:
#     before = time.time()
#     print("Insert a polars dataframe into the feature store")
#     job_execution = local_fg.insert(trans_df)
#     print(
#         "polars_df inserted {} rows into Kafka in {} seconds".format(
#             trans_df.shape[0], time.time() - before
#         )
#     )
# except Exception as e:
#     print(e)

# # %%
# before = time.time()
# job_execution.await_termination()
# print(
#     "Took {} seconds for materialization job to complete".format(time.time() - before)
# )
# # %%

# print("Delete feature group {local_fg.name}_v{local_fg.version}")
# local_fg.delete()
# print("Feature group deleted")