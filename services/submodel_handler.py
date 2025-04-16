from model.app_config import AppConfig, load_config
from services.in_memory_store.in_memory_store import InMemoryStore


class SubmodelHandler:
    def __init__(self):
        self.in_memory_store = InMemoryStore()
        self._app_config: AppConfig = load_config()

    def get_submodels_value_only(self, submodels):
        submodels_value_only = []
        for submodel in submodels:
            r = self.convert_submodel_to_value_only(submodel)
            if r is not None:
                submodels_value_only.append(r)
        return submodels_value_only

    def get_submodel(self, identifier: str):
        submodel = self.in_memory_store.submodel(identifier)

        if submodel is None:
            submodel = self._get_submodel_from_remote(identifier)

        return submodel

    def get_submodel_value_only(self, identifier: str):
        submodel = self.get_submodel(identifier)
        return self.convert_submodel_to_value_only(submodel)

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
        submodel_descriptor = self.in_memory_store.submodel_descriptor(identifier)
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