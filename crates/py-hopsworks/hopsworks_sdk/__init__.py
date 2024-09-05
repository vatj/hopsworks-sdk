from typing import Optional

from hopsworks_sdk import hopsworks_rs
from hopsworks_sdk.platform import project


def login(
    api_key_value: Optional[str] = None,
    project_name: Optional[str] = None,
    url: Optional[str] = None,
) -> project.Project:
    return project.Project._from_pyproj(
        hopsworks_rs.login(
            api_key_value=api_key_value, project_name=project_name, url=url
        )
    )


def version():
    hopsworks_rs.version()
