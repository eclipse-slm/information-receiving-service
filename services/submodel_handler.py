from aas.couch_db_submodel_client import CouchDBSubmodelClient
from model.app_config import AppConfig, load_config
from services.abstract_handler import AbstractHandler
from services.in_memory_store.in_memory_store import InMemoryStore
from services.submodel_descriptor_handler import SubmodelDescriptorHandler


class SubmodelHandler(AbstractHandler):
    def __init__(self):
        self.in_memory_store = None
        if self._use_in_memory_store:
            self.in_memory_store = InMemoryStore()
        self.couch_db_submodel_client = CouchDBSubmodelClient()
        self.submodel_descriptor_handler = SubmodelDescriptorHandler()
        self._app_config: AppConfig = load_config()

    def _total_count(self, aas_source_name: str) -> int:
        if aas_source_name is None:
            return self.couch_db_submodel_client.total_doc_count
        else:
            return self.couch_db_submodel_client.total_view_doc_count(aas_source_name)

    def submodels(self, aas_source_name:str = None, limit: int = -1, cursor: str = "0"):
        if self.in_memory_store:
            start, end = self.get_start_end(limit, cursor)
            if aas_source_name is None:
                return self.in_memory_store.submodels[start:end]
            else:
                submodels_by_source_name, cursor = self.submodel_descriptor_handler.get_submodel_descriptors_by_aas_source_name(aas_source_name, -1, "0")
                submodel_descriptor_ids_by_source_name = [descriptor['id'] for descriptor in submodels_by_source_name]
                return self.submodels_by_ids(submodel_descriptor_ids_by_source_name)[start:end]
        else:
            return self.couch_db_submodel_client.get_submodels(aas_source_name, limit, int(cursor))


    def submodel(self, identifier: str):
        if self.in_memory_store:
            submodel = self.in_memory_store.submodel(identifier)
        else:
            submodel = self.couch_db_submodel_client.get_submodel(identifier)

        if submodel is None:
            submodel = self._get_submodel_from_remote(identifier)

        return submodel


    def submodel_value_only(self, identifier: str):
        return self.convert_submodel_to_value_only(self.submodel(identifier))


    def submodels_by_ids(self, ids: list[str]):
        filtered_submodels = []
        for submodel in self.submodels():
            try:
                if submodel['id'] in ids:
                    filtered_submodels.append(submodel)
            except KeyError:
                continue
        return filtered_submodels


    def get_submodels_by_aas_server_name(self, aas_source_name:str, limit: int, cursor: str):
        submodels = self.submodels(aas_source_name, limit, cursor)
        new_cursor = self.get_cursor(limit, cursor, self._total_count(aas_source_name))
        return submodels, new_cursor
        # if aas_server_name is None:
        #     submodels = self.submodels
        # else:
        #     submodels_by_server_name, cursor = self.submodel_descriptor_handler.get_submodel_descriptors_by_aas_server_name(aas_source_name, -1, "0")
        #     submodel_descriptor_ids_by_server_name = [descriptor['id'] for descriptor in submodels_by_server_name]
        #     submodels = self.submodels_by_ids(submodel_descriptor_ids_by_server_name)
        #
        # start, end, new_cursor = self.get_start_end_cursor(submodels, limit, cursor)
        #
        # return submodels[start:end], new_cursor


    def get_submodels_value_only(self, aas_server_name:str, limit: int, cursor: str):
        submodels = self.get_submodels_by_aas_server_name(aas_server_name, limit, cursor)
        submodels_value_only = []
        for submodel in submodels:
            r = self.convert_submodel_to_value_only(submodel)
            if r is not None:
                submodels_value_only.append(r)
        return submodels_value_only

    # def get_submodel(self, identifier: str):
    #     submodel = self.in_memory_store.submodel(identifier)
    #
    #     if submodel is None:
    #         submodel = self._get_submodel_from_remote(identifier)
    #
    #     return submodel
    #
    # def get_submodel_value_only(self, identifier: str):
    #     submodel = self.get_submodel(identifier)
    #     return self.convert_submodel_to_value_only(submodel)

    def convert_submodel_to_value_only(self, submodel):
        submodel_value_only = {}
        if submodel is None:
            return None

        try:
            for submodel_element in submodel['submodelElements']:
                key = submodel_element['idShort']
                value = self.get_submodel_element_value_only(submodel_element)
                if value is not None:
                    submodel_value_only[key] = value
        except KeyError as e:
            return None

        return submodel_value_only


    def get_submodel_element_value_only(self, submodel_element):
        try:
            value = submodel_element['value']
        except KeyError as e:
            return None

        if submodel_element['modelType'] == 'SubmodelElementList':
            se_list = []
            for v in value:
                se_list.append(self.get_submodel_element_value_only(v))
            return se_list
        elif submodel_element['modelType'] == 'SubmodelElementCollection':
            se_collection = {}
            for v in value:
                se_collection[v['idShort']] = self.get_submodel_element_value_only(v)
            return se_collection
        elif submodel_element['modelType'] == 'MultiLanguageProperty':
            mlp_list = []
            for v in value:
                mlp_list.append({v['language']: v['text']})
            return mlp_list
        else:
            if submodel_element['value'] is not None:
                return value

        return None



    def _get_submodel_from_remote(self, identifier: str):
        submodel_descriptor = self.submodel_descriptor_handler.submodel_descriptor(identifier)
        aasx_server = self._get_responsible_aasx_server_from_submodel_descriptor(submodel_descriptor)

        if aasx_server is None:
            return None

        submodel = aasx_server.request_submodel(identifier)
        return submodel


    def _get_responsible_aasx_server_from_submodel_descriptor(self, submodel_descriptor):
        for aas_server in self._app_config.aas_servers:
            for url in self._get_urls_from_submodel_descriptor(submodel_descriptor):
                if aas_server.url in url:
                    return aas_server
        return None


    def _get_urls_from_submodel_descriptor(self, submodel_descriptor):
        urls = []

        if submodel_descriptor:
            for endpoint in submodel_descriptor['endpoints']:
                if endpoint['protocolInformation']['href']:
                    urls.append(endpoint['protocolInformation']['href'])
        return urls