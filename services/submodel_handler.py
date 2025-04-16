from model.app_config import AppConfig, load_config
from services.in_memory_store.in_memory_store import InMemoryStore


class SubmodelHandler:
    def __init__(self):
        self.in_memory_store = InMemoryStore()
        self._app_config: AppConfig = load_config()

    def get_submodel(self, identifier: str):
        submodel = self.in_memory_store.submodel(identifier)

        if submodel is None:
            submodel = self._get_submodel_from_remote(identifier)

        return submodel

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