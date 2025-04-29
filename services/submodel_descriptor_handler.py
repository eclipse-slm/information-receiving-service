from aas.couch_db_submodel_descriptor_client import CouchDBSubmodelDescriptorClient
from services.abstract_handler import AbstractHandler
from services.in_memory_store.in_memory_store import InMemoryStore


class SubmodelDescriptorHandler(AbstractHandler):
    def __init__(self):
        self.in_memory_store = None
        if self._use_in_memory_store:
            self.in_memory_store = InMemoryStore()
        self.couch_db_submodel_descriptor_client = CouchDBSubmodelDescriptorClient(client_name=self.__class__.__name__)

    @property
    def submodel_descriptors(self):
        if self.in_memory_store:
            return self.in_memory_store.submodel_descriptors
        else:
            return self.couch_db_submodel_descriptor_client.get_all_submodel_descriptors()

    def submodel_descriptor(self, identifier: str):
        for descriptor in self.submodel_descriptors:
            try:
                if descriptor['id'] == identifier:
                    return descriptor
            except KeyError as e:
                continue
        return None


    def submodel_descriptors_by_base_url(self, base_url: str):
        descriptors = []
        for descriptor in self.submodel_descriptors:
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


    def get_submodel_descriptors_by_aas_server_name(self, aas_server_name:str):
        if aas_server_name is None:
            return self.submodel_descriptors
        else:
            base_url = self.get_base_url_by_aas_server_name(aas_server_name)
            return self.submodel_descriptors_by_base_url(base_url)