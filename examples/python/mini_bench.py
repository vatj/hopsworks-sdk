# %%
from hopsworks_sdk import login
import pathlib
import os
import toml
import time
import random
import polars as pl

config_name = "managed-config.toml"
config_path = pathlib.Path(os.getcwd()) / "configs" / config_name
print(config_path)
config = toml.load(config_path)
print(config)

os.environ["HOPSWORKS_API_KEY"] = config["env"]["HOPSWORKS_API_KEY"]
os.environ["HOPOSWORKS_KAFKA_PRODUCER_LOG_DEBUG"] = "broker,topic,msg,queue"
os.environ["HOPSWORKS_KAFKA_PRODUCER_LINGER_MS"] = "10"
os.environ["HOPSWORKS_KAFKA_PRODUCER_QUEUE_BUFFERING_MAX_MS"] = "1000"
os.environ["HOPSWORKS_KAFKA_PRODUCER_BATCH_NUM_MESSAGES"] = "10000"
os.environ["HOPSWORKS_KAFKA_PRODUCER_QUEUE_BUFFERING_MAX_MESSAGES"] = "100000"
os.environ["HOPSWORKS_KAFKA_PRODUCER_QUEUE_BUFFERING_MAX_KBYTES"] = "1048576"

project = login()
fs = project.get_feature_store()

# %%
trans_df = pl.read_csv(
    "https://repo.hops.works/master/hopsworks-tutorials/data/card_fraud_data/transactions.csv",
    try_parse_dates=True,
)
print(trans_df.head(5))

# %%

name = "".join(random.choices(["a", "b", "c", "d", "e", "f", "g", "h"], k=5))

version = 1
local_fg = fs.get_or_create_feature_group(
    name=name,
    version=version,
    description="benchmark_fg",
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
    before = time.time()
    print("Insert a polars dataframe into the feature store")
    job_execution = local_fg.insert(trans_df)
    print("polars_df inserted {} rows into Kafka in {} seconds".format(trans_df.shape[0], time.time() - before))
    job_execution.await_termination()
    print("Took {} seconds for materialization job to complete".format(time.time()))
except Exception as e:
    print(e)

# %%

# %%
# try:
#     before = time.time()
#     print("Read from online feature store to arrow record batch")
#     arrow_rb = local_fg.read_from_online_store(return_type="pyarrow")
#     print(
#         "Took {} seconds to read {} rows from online store".format(
#             time.time() - before, arrow_rb.shape[0]
#         )
#     )
#     print("arrow_rb : \n", arrow_rb.head(5))
    
# except Exception as e:
#     print(e)

# # %%
# try:
#     before = time.time()
#     print("Read from online feature store to polars dataframe")
#     polars_df = local_fg.read_from_online_store(return_type="polars")
#     print(
#         "Took {} seconds to read {} rows".format(
#             time.time() - before, polars_df.shape[0]
#         )
#     )
# except Exception as e:
#     print(e)


# # %%
# try:
#     before = time.time()
#     print("Get the record batch from the offline store")
#     arrow_rb = local_fg.read_from_offline_store(return_type="pyarrow")
#     print(
#         "Took {} seconds to read {} rows".format(
#             time.time() - before, arrow_rb.shape[0]
#         )
#     )
#     print("arrow_rb : ", arrow_rb.head(5))
# except Exception as e:
#     print(e)

# # %%
# try:
#     before = time.time()
#     print("Get the polars dataframe from the offline store")
#     polars_df = local_fg.read_from_offline_store(return_type="polars")
#     print(
#         "Took {} seconds to read {} rows".format(
#             time.time() - before, polars_df.shape[0]
#         )
#     )
# except Exception as e:
#     print(e)

# %%
print("Delete feature group {local_fg.name}_v{local_fg.version}")
local_fg.delete()
print("Feature group deleted")