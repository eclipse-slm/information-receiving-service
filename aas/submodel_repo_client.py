import os

from aas_python_http_client import Configuration, AssetAdministrationShellRepositoryAPIApi, ApiClient, \
    SubmodelRepositoryAPIApi


class SubmodelRepoClient(SubmodelRepositoryAPIApi):
    def __init__(self):
        submodel_repo_config = Configuration()
        submodel_repo_config.host = os.getenv("AAS_SUBMODEL_REPOSITORY_HOST")

        self.api_client=ApiClient(configuration=submodel_repo_config)
