import logging
import threading
from typing import List

from dotenv import load_dotenv
from pycouchdb import exceptions

from aas.couch_db_shell_client import CouchDBShellClient
from aas.couch_db_shell_descriptor_client import CouchDBShellDescriptorClient
from aas.couch_db_submodel_client import CouchDBSubmodelClient
from aas.couch_db_submodel_descriptor_client import CouchDBSubmodelDescriptorClient
from model.aas_source import AasSource
from model.app_config import load_config

load_dotenv()

class GarbageCollector:
    log = logging.getLogger(__name__)

    def __init__(self):
        self._run_garbage_collecting: bool = True
        self._latest_aas_sources: List = []
        self._couchdb_shell_descriptor_client = CouchDBShellDescriptorClient(client_name=self.__class__.__name__)
        self._couchdb_shell_client = CouchDBShellClient()
        self._couchdb_submodel_descriptor_client = CouchDBSubmodelDescriptorClient(client_name=self.__class__.__name__)
        self._couchdb_submodel_client = CouchDBSubmodelClient()

        self._start_garbage_collecting()

    @property
    def _aas_sources(self) -> List[AasSource]:
        config = load_config()
        aas_sources = []
        for aas_source in config.aas_servers:
            aas_sources.append(aas_source)
        aas_sources_names = [aas_source.name for aas_source in aas_sources]

        if aas_sources_names != self._latest_aas_sources or len(aas_sources_names) != len(self._latest_aas_sources):
            self._latest_aas_sources = aas_sources_names
            self._log(f"AAS sources changed | {self._latest_aas_sources}")

        return aas_sources

    @property
    def _shell_registry_base_urls(self):
        return [aas_source.shell_registry_base_url for aas_source in self._aas_sources if aas_source.shell_registry_base_url is not None]

    @property
    def _shell_repository_base_urls(self):
        return [aas_source.shell_repository_base_url for aas_source in self._aas_sources if aas_source.shell_repository_base_url is not None]

    @property
    def _submodel_registry_base_urls(self):
        return [aas_source.submodel_registry_base_url for aas_source in self._aas_sources if aas_source.submodel_registry_base_url is not None]

    @property
    def _submodel_repository_base_urls(self):
        return [aas_source.submodel_repository_base_url for aas_source in self._aas_sources if aas_source.submodel_repository_base_url is not None]


    def _start_garbage_collecting(self):
        self._start_garbage_collecting_by_id_request()


    def _start_garbage_collecting_by_endpoints(self):
        self._log(f"Start garbage collecting by endpoints")
        thread1 = threading.Thread(target=self._do_shell_garbage_collecting_by_endpoints)
        thread2 = threading.Thread(target=self._do_submodel_garbage_collecting_by_endpoints)
        threads = [thread1, thread2]

        for thread in threads:
            thread.start()


    def _start_garbage_collecting_by_id_request(self):
        self._log(f"By Id Request | Start garbage collecting | Initial Sources: {[source.name for source in self._aas_sources]}")
        thread1 = threading.Thread(target=self._do_shell_descriptor_garbage_collecting_by_id_request)
        thread2 = threading.Thread(target=self._do_shell_garbage_collecting_by_id_request)
        thread3 = threading.Thread(target=self._do_submodel_descriptor_garbage_collecting_by_id_request)
        thread4 = threading.Thread(target=self._do_submodel_garbage_collecting_by_id_request)
        threads = [thread1, thread2, thread3, thread4]

        while self._run_garbage_collecting:
            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()


    def _do_shell_descriptor_garbage_collecting_by_id_request(self):
        self._log(f"By Id Request | Start shell descriptor collecting")
        shell_descriptors = self._couchdb_shell_descriptor_client.get_all_shell_descriptors()

        for shell_descriptor in shell_descriptors:
            found = False
            for aas_source in self._aas_sources:
                result = aas_source.request_shell_descriptor(shell_descriptor.id)
                if result is not None:
                    found = True
                    break

            if not found:
                self._log(f"By Id Request | Delete shell descriptor | id = {shell_descriptor.id}")
                self._couchdb_shell_descriptor_client.delete_shell_descriptor(shell_descriptor.id)


    def _do_shell_garbage_collecting_by_id_request(self):
        self._log(f"By Id Request | Start shell collecting")
        shells = self._couchdb_shell_client.get_all_shells()

        for shell in shells:
            found = False
            for aas_source in self._aas_sources:
                result = aas_source.request_shell(shell['id'])
                if result is not None:
                    found = True
                    break

            if not found:
                self._log(f"By Id Request | Delete shell | id = {shell['id']}")
                self._couchdb_shell_client.delete_shell(shell['id'])


    def _do_submodel_descriptor_garbage_collecting_by_id_request(self):
        self._log(f"By Id Request | Submodel descriptor collecting")
        submodel_descriptors = self._couchdb_submodel_descriptor_client.get_all_submodel_descriptors()
        shells = self._couchdb_shell_client.get_all_shells()

        for submodel_descriptor in submodel_descriptors:
            # Get shells containing the submodel descriptor
            shells_containing_descriptor = self._filter_shells_by_submodel_descriptor(shells, submodel_descriptor)

            found = False
            for aas_source in self._aas_sources:
                for shell in shells_containing_descriptor:
                    result = aas_source.request_submodel_descriptor(
                        shell['id'],
                        submodel_descriptor.id
                    )
                    if result is not None:
                        found = True
                        break
                if found:
                    break

            if not found:
                self._log(f"By Id Request | Delete submodel descriptor | id = {submodel_descriptor.id}")
                self._couchdb_submodel_descriptor_client.delete_submodel_descriptor(submodel_descriptor.id)


    def _do_submodel_garbage_collecting_by_id_request(self):
        self._log(f"By Id Request | Submodel collecting")
        submodels = self._couchdb_submodel_client.get_all_submodels()

        for submodel in submodels:
            found = False
            for aas_source in self._aas_sources:
                result = aas_source.request_submodel(submodel['id'])
                if result is not None:
                    found = True
                    break

            if not found:
                self._log(f"By Id Request | Delete submodel | id = {submodel['id']}")
                self._couchdb_submodel_client.delete_submodel(submodel['id'])


    def _do_shell_garbage_collecting_by_endpoints(self):
        while self._run_garbage_collecting:
            self._log(f"By Endpoints | Shell collecting")
            self._log(f"By Endpoints | Shell registry base urls | {self._shell_registry_base_urls}")
            shell_descriptors = self._couchdb_shell_descriptor_client.get_all_shell_descriptors()

            for shell_descriptor in shell_descriptors:
                if isinstance(shell_descriptor, dict):
                    endpoints = shell_descriptor['endpoints']
                    id = shell_descriptor['id']
                else:
                    endpoints = shell_descriptor.endpoints
                    id = shell_descriptor.id

                if not self._is_base_url_in_endpoint_list(
                        endpoints,
                        self._shell_registry_base_urls
                ):
                    self._log(f"By Endpoints | Delete shell descriptor | id = {id}")
                    self._couchdb_shell_descriptor_client.delete_shell_descriptor(id)
                    self._log(f"By Endpoints | Delete shell | id = {id}")
                    self._couchdb_shell_client.delete_shell(id)


    def _do_submodel_garbage_collecting_by_endpoints(self):
        while self._run_garbage_collecting:
            self._log(f"By Endpoints | Start submodel collecting")
            self._log(f"By Endpoints | Submodel registry base urls | {self._submodel_registry_base_urls}")
            submodel_descriptors = self._couchdb_submodel_descriptor_client.get_all_submodel_descriptors()

            for submodel_descriptor in submodel_descriptors:
                if isinstance(submodel_descriptor, dict):
                    endpoints = submodel_descriptor['endpoints']
                    id = submodel_descriptor['id']
                else:
                    endpoints = submodel_descriptor.endpoints
                    id = submodel_descriptor.id

                if not self._is_base_url_in_endpoint_list(
                        endpoints,
                        self._shell_registry_base_urls
                ):
                    try:
                        self._log(f"By Endpoints | Delete submodel descriptor | id = {id}")
                        self._couchdb_submodel_descriptor_client.delete_submodel_descriptor(id)
                        self._log(f"By Endpoints | Delete submodel | id = {id}")
                        self._couchdb_submodel_client.delete_submodel(id)
                    except exceptions.NotFound as e:
                        continue
                    except exceptions.Conflict as e:
                        self._log(f"By Endpoints | Failed to delete submodel | id = {id} | {e}")


    def _filter_shells_by_submodel_descriptor(self, shells, submodel_descriptor):
        filtered_shells = []

        for shell in shells:
            try:
                submodels = shell['submodels']
                for submodel in submodels:
                    keys = [key['value'] for key in submodel['keys']]
                    if submodel_descriptor.id in keys:
                        filtered_shells.append(shell)
                        break
            except KeyError:
                continue

        return filtered_shells


    def _is_base_url_in_endpoint_list(self, endpoints, base_urls) -> bool:
        for endpoint in endpoints:
            if isinstance(endpoint, dict):
                href = endpoint['protocol_information']['href']
            else:
                href = endpoint.protocol_information.href
            for base_url in base_urls:
                if base_url in href:
                    return True
        return False


    def _log(self, message: str, level: int = logging.INFO):
        if level == logging.DEBUG:
            self.log.debug(f"{message}")
        elif level == logging.ERROR:
            self.log.error(f"{message}")
        else:
            self.log.info(f"{message}")

