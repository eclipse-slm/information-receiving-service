import os

from aas_python_http_client import SubmodelRegistryAPIApi, Configuration, ApiClient


class SubmodelRegistryClient(SubmodelRegistryAPIApi):
    def __init__(self):
        submodel_registry_config = Configuration()
        submodel_registry_config.host = os.getenv("AAS_SUBMODEL_REGISTRY_HOST")
        self.api_client = ApiClient(configuration=submodel_registry_config)