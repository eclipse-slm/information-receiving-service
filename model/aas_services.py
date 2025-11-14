from aas_python_http_client import Configuration, ApiClient, AssetAdministrationShellRegistryAPIApi, \
    AssetAdministrationShellRepositoryAPIApi, SubmodelRegistryAPIApi, SubmodelRepositoryAPIApi
from pydantic import BaseModel, Field

from model.aas_source import AasSource


class AasServiceUrls(BaseModel):
    shell_registry: str = Field(alias="shell-registry", default=None)
    shell_repository: str = Field(alias="shell-repository", default=None)
    submodel_registry: str = Field(alias="submodel-registry", default=None)
    submodel_repository: str = Field(alias="submodel-repository", default=None)


class AasServices(AasSource):
    name: str
    urls: AasServiceUrls

    @property
    def shell_registry_base_url(self) -> str:
        return self.urls.shell_registry

    @property
    def shell_repository_base_url(self) -> str:
        return self.urls.shell_repository

    @property
    def submodel_registry_base_url(self) -> str:
        return self.urls.submodel_registry

    @property
    def submodel_repository_base_url(self) -> str:
        return self.urls.submodel_repository

    def api_client(self, url: str):
        return ApiClient(
            configuration=self.api_configuration(url),
        )

    @property
    def shell_registry_client(self):
        if self.urls.shell_registry is None:
            return None
        return AssetAdministrationShellRegistryAPIApi(
            api_client=self.api_client(self.urls.shell_registry)
        )


    @property
    def shell_repository_client(self):
        if self.urls.shell_repository is None:
            return None
        return AssetAdministrationShellRepositoryAPIApi(
            api_client=self.api_client(self.urls.shell_repository)
        )


    @property
    def submodel_registry_client(self):
        if self.urls.submodel_registry is None:
            return None
        return SubmodelRegistryAPIApi(
            api_client=self.api_client(self.urls.submodel_registry)
        )


    @property
    def submodel_repository_client(self):
        if self.urls.submodel_repository is None:
            return None
        return SubmodelRepositoryAPIApi(
            api_client=self.api_client(self.urls.submodel_repository)
        )