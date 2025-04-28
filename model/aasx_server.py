from typing import List, Optional, Union, Any

from aas_python_http_client import ApiClient, AssetAdministrationShellRegistryAPIApi, Configuration, \
    SubmodelRegistryAPIApi, \
    AssetAdministrationShellRepositoryAPIApi, \
    SubmodelRepositoryAPIApi
from pydantic import Field, model_validator
from pydantic.v1 import root_validator
from typing_extensions import Self

from model.aas_source import AasSource
from model.auth import Oauth2AuthMethod, ApiKeyAuthMethod, CustomOAuthMethod


class AasxServer(AasSource):
    name: str
    id_link_prefix: str = Field(alias="id-link-prefix", default=None)
    aas_endpoint_prefixes: Optional[List[str]] = Field(alias="aas-endpoint-prefixes", default=None)
    url: str
    discovery_url: Optional[str] = Field(alias="discovery-url", default=None)
    version: Optional[str] = Field(alias="version", default=None)
    auth: Union[Oauth2AuthMethod, ApiKeyAuthMethod, CustomOAuthMethod]

    @model_validator(mode="after")
    def check_auth(self) -> Self:
        self.auth.auth_header
        return self


    @property
    def api_client(self) -> ApiClient:
        auth_header_key = next(iter(self.auth.auth_header))
        auth_header_value = self.auth.auth_header[auth_header_key]
        return ApiClient(
            configuration=self.api_configuration(self.url),
            header_name=auth_header_key,
            header_value=auth_header_value
        )

    @property
    def shell_registry_base_url(self) -> str:
        return self.url

    @property
    def shell_repository_base_url(self) -> str:
        return self.url

    @property
    def submodel_registry_base_url(self) -> str:
        return self.url

    @property
    def submodel_repository_base_url(self) -> str:
        return self.url


    @property
    def shell_registry_client(self):
        return AssetAdministrationShellRegistryAPIApi(api_client=self.api_client)


    @property
    def shell_repository_client(self):
        return AssetAdministrationShellRepositoryAPIApi(api_client=self.api_client)


    @property
    def submodel_registry_client(self):
        return SubmodelRegistryAPIApi(api_client=self.api_client)


    @property
    def submodel_repository_client(self):
        return SubmodelRepositoryAPIApi(api_client=self.api_client)

