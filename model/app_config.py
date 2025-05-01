import os
from typing import Union, List, Any

import yaml
from dotenv import load_dotenv
from oauthlib.oauth2 import MissingTokenError
from pydantic import BaseModel, Field, model_validator, ValidationError

from model.aas_services import AasServices
from model.aasx_server import AasxServer

load_dotenv()

aas_source_classes = [AasxServer, AasServices]

class AppConfig(BaseModel):
    aas_servers: List[Union[tuple(aas_source_classes)]] = Field(alias = "aas-servers")

    @model_validator(mode="before")
    @classmethod
    def check_integrity_of_source_configs(cls, data: Any):
        aas_sources = {"aas-servers": []}
        for source_dict in data['aas-servers']:
            if cls._is_instance_of_aas_source(source_dict):
                aas_sources['aas-servers'].append(source_dict)
            else:
                continue
                # print(f"Invalid AAS source configuration of entry with name: {source_dict['name']} | Ignoring configuration")
        return aas_sources

    @classmethod
    def _is_instance_of_aas_source(cls, source_dict: dict) -> bool:
        for aas_source_class in aas_source_classes:
            try:
                aas_source_class(**source_dict)
                return True
            except ValidationError as e:
                print(f"Validation error of aas source \"{source_dict['name']}\"")
                continue
            except MissingTokenError as e:
                print(f"Missing token error of aas source \"{source_dict['name']}\"")
                continue
        return False


def load_config() -> AppConfig:
    file_path = os.getenv("APP_CONFIG_PATH", "config.yml")
    return load_config_from_path(file_path)


def load_config_from_path(path: str = "config.yml") -> AppConfig:
    with open(path, 'r') as file:
        config_data = yaml.safe_load(file)
    return AppConfig(**config_data)