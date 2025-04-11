import json
from typing import List, Optional, Union

from aas_python_http_client import ApiClient, AssetAdministrationShellRegistryAPIApi, Configuration, \
    GetAssetAdministrationShellDescriptorsResult, AssetAdministrationShellDescriptor, SubmodelRegistryAPIApi, \
    AssetAdministrationShellRepositoryAPIApi, \
    AssetAdministrationShell, SubmodelRepositoryAPIApi, Submodel
from pydantic import BaseModel, Field

from model.auth import Oauth2AuthMethod, ApiKeyAuthMethod, CustomOAuthMethod
from services.aas_utils import encode_id


class AasxServer(BaseModel):
    name: str
    id_link_prefix: str = Field(alias="id-link-prefix")
    aas_endpoint_prefixes: Optional[List[str]] = Field(alias="aas-endpoint-prefixes", default=None)
    url: str
    discovery_url: Optional[str] = Field(alias="discovery-url", default=None)
    version: Optional[str] = Field(alias="version", default=None)
    auth: Union[Oauth2AuthMethod, ApiKeyAuthMethod, CustomOAuthMethod]
    _limit: int = 45000


    # @property
    # def shell_descriptors(self) -> List[AssetAdministrationShellDescriptor]:
    #     if not self._shell_descriptors:
    #         self.request_shell_descriptors()
    #     return self._shell_descriptors


    @property
    def api_client(self):
        configuration = Configuration()
        configuration.host = self.url
        auth_header_key = next(iter(self.auth.auth_header))
        auth_header_value = self.auth.auth_header[auth_header_key]
        return ApiClient(
            configuration=configuration,
            header_name=auth_header_key,
            header_value=auth_header_value
        )


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


    def _request_shell_descriptors(self, cursor: str, limit: int) -> GetAssetAdministrationShellDescriptorsResult:
        if cursor == "0":
            return self.shell_registry_client.get_all_asset_administration_shell_descriptors(limit=limit)
        else:
            return self.shell_registry_client.get_all_asset_administration_shell_descriptors(cursor=cursor, limit=limit)


    def _request_shells(self, cursor: str, limit: int) -> dict:
        if cursor == "0":
            response = self.shell_repository_client.get_all_asset_administration_shells(
                limit=limit,
                _preload_content=False,
                _return_http_data_only=True
            )
        else:
            response = self.shell_repository_client.get_all_asset_administration_shells(
                cursor=cursor,
                limit=limit,
                _preload_content=False,
                _return_http_data_only=True
            )

        return json.loads(response.data)


    def _request_all_submodels(self) -> dict:
        limit = 100
        cursor = 0
        result = None

        while cursor is not None:
            response_json = self._request_submodels(str(cursor), limit)
            try:
                cursor = response_json["paging_metadata"]["cursor"]
            except KeyError:
                cursor = None

            if result is None:
                result = response_json
            else:
                result["result"].extend(response_json["result"])

        result["paging_metadata"] = {}
        return result


    def _request_submodels(self, cursor: str, limit: int) -> dict:
        if cursor == "0":
            response = self.submodel_repository_client.get_all_submodels(
                limit=limit,
                _preload_content=False,
                _return_http_data_only=True
            )
        else:
            response = self.submodel_repository_client.get_all_submodels(
                cursor=cursor,
                limit=limit,
                _preload_content=False,
                _return_http_data_only=True
            )

        return json.loads(response.data)


    def _request_submodel(self, identifier: str):
        response = self.submodel_repository_client.get_submodel_by_id(
            submodel_identifier=encode_id(identifier),
            _preload_content=False,
            _return_http_data_only=True
        )
        return json.loads(response.data)

    def _request_submodel_descriptors(self, cursor: str, limit: int) -> GetAssetAdministrationShellDescriptorsResult:
        if cursor == "0":
            return self.submodel_registry_client.get_all_submodel_descriptors(limit=limit)
        else:
            return self.submodel_registry_client.get_all_submodel_descriptors(cursor=cursor, limit=limit)


    def request_shell_descriptors(self) -> List[AssetAdministrationShellDescriptor]:
        response = self._request_shell_descriptors(str(0), self._limit)
        return response.result

    def request_shells(self) -> List[AssetAdministrationShell]:
        response = self._request_shells(str(0), self._limit)
        # self._shells = response.result
        return response['result']


    def request_submodels(self) -> List[Submodel]:
        response = self._request_all_submodels()
        # self._shells = response.result
        return response['result']


    def request_submodel(self, identifier: str) -> Optional[Submodel]:
        response = self._request_submodel(identifier)
        return response

