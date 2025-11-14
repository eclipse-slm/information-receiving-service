import os

from aas_python_http_client import AssetAdministrationShellRegistryAPIApi, Configuration, ApiClient


class ShellRegistryClient(AssetAdministrationShellRegistryAPIApi):
    def __init__(self):
        shell_registry_config = Configuration()
        shell_registry_config.host = os.getenv("AAS_SHELL_REGISTRY_HOST")
        self.api_client = ApiClient(configuration=shell_registry_config)

