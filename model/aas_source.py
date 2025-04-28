import json
from typing import List, Optional

from aas_python_http_client import ApiClient, AssetAdministrationShellRegistryAPIApi, \
    AssetAdministrationShellRepositoryAPIApi, SubmodelRegistryAPIApi, GetAssetAdministrationShellDescriptorsResult, \
    AssetAdministrationShellDescriptor, AssetAdministrationShell, Submodel, SubmodelDescriptor, \
    GetSubmodelDescriptorsResult, Configuration
from aas_python_http_client.rest import ApiException
from pydantic import BaseModel

from services.aas_utils import encode_id


class AasSource(BaseModel):
    polling_interval_s: int = -1
    _limit: int = 45000


    def api_configuration(self, url: str) -> Configuration:
        if url is None:
            return None
        configuration = Configuration()
        configuration.host = url
        return configuration


    def api_client(self) -> ApiClient:
        pass

    @property
    def shell_registry_client(self) -> AssetAdministrationShellRegistryAPIApi:
        pass


    @property
    def shell_repository_client(self) -> AssetAdministrationShellRepositoryAPIApi:
        pass


    @property
    def submodel_registry_client(self) -> SubmodelRegistryAPIApi:
        pass


    @property
    def submodel_repository_client(self) -> SubmodelRegistryAPIApi:
        pass


    def _request_shell_descriptors(self, cursor: str, limit: int) -> GetAssetAdministrationShellDescriptorsResult:
        if cursor == "0":
            return self.shell_registry_client.get_all_asset_administration_shell_descriptors(limit=limit)
        else:
            return self.shell_registry_client.get_all_asset_administration_shell_descriptors(cursor=cursor, limit=limit)


    def request_shell_descriptor(self, identifier: str) -> AssetAdministrationShellDescriptor:
        if self.shell_registry_client is None:
            return None

        try:
            return self.shell_registry_client.get_asset_administration_shell_descriptor_by_id(
                encode_id(identifier)
            )
        except (TypeError, ApiException) as e:
            return None


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


    def request_shell(self, identifier: str) -> AssetAdministrationShell:
        if self.shell_repository_client is None:
            return None

        try:
            result = self.shell_repository_client.get_asset_administration_shell_by_id(
                encode_id(identifier)
            )

            # Some aasx server do not return 404 but 200 with error message
            try:
                if any(k.lower() == "messages" for k in result.keys()):
                    return None
            except AttributeError:
                pass

            return result

            return result
        except (TypeError, ApiException) as e:
            return None


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

    def _request_submodel_descriptors(self, cursor: str, limit: int) -> GetSubmodelDescriptorsResult:
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


    def request_submodel_descriptors(self) -> List[SubmodelDescriptor]:
        response = self._request_submodel_descriptors(str(0), self._limit)
        return response.result


    def request_submodel_descriptor(self, aas_identifier: str, sm_identifier: str) -> SubmodelDescriptor:
        if self.submodel_registry_client is None:
            return None

        try:
            return self.shell_registry_client.get_submodel_descriptor_by_id_through_superpath(
                encode_id(aas_identifier),
                encode_id(sm_identifier),
                _preload_content=False
            )
        except (TypeError, ApiException) as e:
            return None


    def request_submodels(self) -> List[Submodel]:
        response = self._request_all_submodels()
        # self._shells = response.result
        return response['result']


    def request_submodel(self, identifier: str) -> Optional[Submodel]:
        try:
            if self.submodel_repository_client is None:
                return None

            result = self._request_submodel(identifier)
            # Some aasx server do not return 404 but 200 with error message
            try:
                if any(k.lower() == "messages" for k in result.keys()):
                    return None
            except AttributeError:
                pass

            return result
        except (ApiException, TypeError) as e:
            return None