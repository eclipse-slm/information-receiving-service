import os

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field

from model.aasx_server import AasxServer

load_dotenv()


class AppConfig(BaseModel):
    aas_servers: list[AasxServer] = Field(alias = "aas-servers")


def load_config() -> AppConfig:
    file_path = os.getenv("APP_CONFIG_PATH", "config.yml")
    return load_config_from_path(file_path)


def load_config_from_path(path: str = "config.yml") -> AppConfig:
    with open(path, 'r') as file:
        config_data = yaml.safe_load(file)
    return AppConfig(**config_data)