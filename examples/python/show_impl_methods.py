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
    FORMAT = "%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s"
    logging.basicConfig(format=FORMAT)
    logging.getLogger().setLevel(
        logging.DEBUG if config["env"]["RUST_LOG"] else logging.INFO
    )
    logging.info("RUST_LOG set to %s", config["env"]["RUST_LOG"])

project = login()
fs = project.get_feature_store()


print("Project object methods:")
print([method for method in dir(project) if not method.startswith("__")])

print("PyProject object methods:")
print([method for method in dir(project._proj) if not method.startswith("__")])

print("FeatureStore object methods:")
print([method for method in dir(fs) if not method.startswith("__")])

print("PyFeatureStore object methods:")
print([method for method in dir(fs._fs) if not method.startswith("__")])

if config["env"].get("HOPSWORKS_FEATURE_GROUP_NAME") is not None:
    fg = fs.get_feature_group(config["env"].get("HOPSWORKS_FEATURE_GROUP_NAME"), 1)

    print("FeatureGroup object methods:")
    print([method for method in dir(fg) if not method.startswith("__")])

    print("PyFeatureGroup object methods:")
    print([method for method in dir(fg._fg) if not method.startswith("__")])
