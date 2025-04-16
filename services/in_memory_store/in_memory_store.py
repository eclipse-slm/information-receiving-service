import concurrent.futures
from typing import List

from aas_python_http_client import SubmodelDescriptor, AssetAdministrationShellDescriptor
from basyx.aas.model import AssetAdministrationShell, Submodel

from model.aasx_server import AasxServer
from model.app_config import load_config
from services.in_memory_store.in_memory_store_shell_descriptors import InMemoryStoreShellDescriptors
from services.in_memory_store.in_memory_store_shells import InMemoryStoreShells
from services.in_memory_store.in_memory_store_submodel_descriptors import InMemoryStoreSubmodelDescriptor
from services.in_memory_store.in_memory_store_submodels import InMemoryStoreSubmodels


def singleton(cls):
    instances = {}

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance


@singleton
class InMemoryStore:
    def __init__(self):
        self.stores = []
        self._aasx_servers: list[AasxServer] = load_config().aas_servers
        self._init_stores()

    def _init_stores(self):
        if len(self.stores) > 0:
            return

        with concurrent.futures.ThreadPoolExecutor() as executor:
            tasks = [
                executor.submit(InMemoryStoreShellDescriptors.create_store),
                executor.submit(InMemoryStoreShells.create_store),
                executor.submit(InMemoryStoreSubmodelDescriptor.create_store),
                executor.submit(InMemoryStoreSubmodels.create_store),
            ]
            for future in concurrent.futures.as_completed(tasks):
                result = future.result()
                self.stores.append(result)

    def _get_store(self, class_type):
        for store in self.stores:
            if isinstance(store, class_type):
                return store
        return None

    def _get_start_end_cursor(self, aas_objects: List[dict], limit: int, cursor: str):
        start = int(cursor) if cursor is not None else 0
        end = start + limit
        new_cursor = str(end) if end < len(aas_objects) else None
        return start, end, new_cursor

    @property
    def shell_descriptors(self) -> List[AssetAdministrationShellDescriptor]:
        return self._get_store(InMemoryStoreShellDescriptors).store


    def get_shell_descriptors_by_aas_server_name(self, aas_server_name:str, limit: int, cursor: str):
        if aas_server_name is not None:
            base_url = self.get_base_url_by_aas_server_name(aas_server_name)
            shell_descriptors = self._get_store(InMemoryStoreShellDescriptors).get_store_filtered(base_url)
        else:
            shell_descriptors = self.shell_descriptors

        start, end, new_cursor = self._get_start_end_cursor(shell_descriptors, limit, cursor)

        if limit == -1:
            return shell_descriptors, None
        return shell_descriptors[start:end], new_cursor


    def shell_descriptor(self, identifier: str) -> AssetAdministrationShellDescriptor:
        return self._get_store(InMemoryStoreShellDescriptors).shell_descriptor(identifier)

    @property
    def shells(self, company_name:str = None) -> List[AssetAdministrationShell]:
        if company_name is None:
            return self._get_store(InMemoryStoreShells).store
        else:
            base_url = self.get_base_url_by_aas_server_name(company_name)
            return self._get_store(InMemoryStoreShells).get_store_filtered(base_url)


    def get_shells_by_aas_server_name(self, aas_server_name:str, limit: int, cursor: str) -> List[AssetAdministrationShell]:
        if aas_server_name is None:
            shells = self.shells
        else:
            descriptors_by_server_name, cursor = self.get_shell_descriptors_by_aas_server_name(aas_server_name, -1, "0")
            shell_descriptor_ids_by_server_name = [descriptor['id'] for descriptor in descriptors_by_server_name]
            shells = self._get_store(InMemoryStoreShells).get_store_filtered(shell_descriptor_ids_by_server_name)

        start, end, new_cursor = self._get_start_end_cursor(shells, limit, cursor)

        return shells[start:end], new_cursor


    def shell(self, identifier: str) -> AssetAdministrationShell:
        return self._get_store(InMemoryStoreShells).shell(identifier)

    @property
    def submodel_descriptors(self) -> List[SubmodelDescriptor]:
        return self._get_store(InMemoryStoreSubmodelDescriptor).store


    def get_submodel_descriptors_by_aas_server_name(self, aas_server_name:str) -> List[AssetAdministrationShellDescriptor]:
        if aas_server_name is None:
            return self.submodel_descriptors
        else:
            base_url = self.get_base_url_by_aas_server_name(aas_server_name)
            return self._get_store(InMemoryStoreSubmodelDescriptor).get_store_filtered(base_url)


    def submodel_descriptor(self, identifier: str) -> SubmodelDescriptor:
        return self._get_store(InMemoryStoreSubmodelDescriptor).submodel_descriptor(identifier)

    @property
    def submodels(self) -> List[Submodel]:
        return self._get_store(InMemoryStoreSubmodels).store


    def get_submodels_by_aas_server_name(self, aas_server_name:str, limit: int, cursor: str) -> List[AssetAdministrationShell]:
        if aas_server_name is None:
            submodels = self.submodels
        else:
            submodels_by_server_name, cursor = self.get_submodel_descriptors_by_aas_server_name(aas_server_name, -1, "0")
            submodel_descriptor_ids_by_server_name = [descriptor['id'] for descriptor in submodels_by_server_name]
            submodels = self._get_store(InMemoryStoreSubmodels).get_store_filtered(submodel_descriptor_ids_by_server_name)

        start, end, new_cursor = self._get_start_end_cursor(submodels, limit, cursor)

        return submodels[start:end], new_cursor


    def submodel(self, identifier: str) -> Submodel:
        return self._get_store(InMemoryStoreSubmodels).submodel(identifier)


    def get_base_url_by_aas_server_name(self, aas_server_name:str):
        for aasx_server in self._aasx_servers:
            if aasx_server.name.lower() == aas_server_name.lower():
                return aasx_server.url
        return None

