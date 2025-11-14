from aas.couch_db_submodel_descriptor_client import CouchDBSubmodelDescriptorClient
from services.abstract_handler import AbstractHandler
from services.in_memory_store.in_memory_store import InMemoryStore


class SubmodelDescriptorHandler(AbstractHandler):
    def __init__(self):
        self.in_memory_store = None
        if self._use_in_memory_store:
            self.in_memory_store = InMemoryStore()
        self.couch_db_submodel_descriptor_client = CouchDBSubmodelDescriptorClient(client_name=self.__class__.__name__)

    def _total_count(self, aas_source_name: str) -> int:
        if aas_source_name is None:
            return self.couch_db_submodel_descriptor_client.total_doc_count
        else:
            return self.couch_db_submodel_descriptor_client.total_view_doc_count(aas_source_name)


    def submodel_descriptors(self, aas_source_name:str = None, limit: int = -1, cursor: str = "0"):
        if self.in_memory_store:
            start, end = self.get_start_end(limit, cursor)
            if aas_source_name is not None:
                base_url = self.get_base_url_by_aas_server_name(aas_source_name)
                return self.submodel_descriptors_by_base_url(base_url)[start:end]
            else:
                return self.in_memory_store.submodel_descriptors[start:end]
        else:
            return self.couch_db_submodel_descriptor_client.get_submodel_descriptors(
                aas_source_name,
                limit,
                int(cursor)
            )

    def submodel_descriptor(self, identifier: str):
        for descriptor in self.submodel_descriptors():
            try:
                if descriptor['id'] == identifier:
                    return descriptor
            except KeyError as e:
                continue
        return None


    def submodel_descriptors_by_base_url(self, base_url: str):
        descriptors = []
        for descriptor in self.submodel_descriptors():
            if isinstance(descriptor, dict):
                try:
                    if self.endpoints_contain_base_url(descriptor['endpoints'], base_url):
                        descriptors.append(descriptor)
                except KeyError:
                    continue
            else:
                if self.endpoints_contain_base_url(descriptor.endpoints, base_url):
                    descriptors.append(descriptor)
        return descriptors


    def get_submodel_descriptors_by_aas_source_name(self, aas_source_name:str, limit: int, cursor: str):
        submodel_descriptors = self.submodel_descriptors(aas_source_name, limit, cursor)
        cursor = self.get_cursor(limit, cursor, self._total_count(aas_source_name))
        return submodel_descriptors, cursor
        # if aas_server_name is None:
        #     return self.submodel_descriptors
        # else:
        #     base_url = self.get_base_url_by_aas_server_name(aas_source_name)
        #     return self.submodel_descriptors_by_base_url(base_url)