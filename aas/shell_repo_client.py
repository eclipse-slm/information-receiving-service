import os

from aas_python_http_client import Configuration, AssetAdministrationShellRepositoryAPIApi, ApiClient, \
    SubmodelRepositoryAPIApi


class ShellRepoClient(AssetAdministrationShellRepositoryAPIApi):
    def __init__(self):
        shell_repo_config = Configuration()
        shell_repo_config.host = os.getenv("AAS_SHELL_REPOSITORY_HOST")
        self.api_client = ApiClient(configuration=shell_repo_config)


