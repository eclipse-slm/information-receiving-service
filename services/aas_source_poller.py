import json
import logging
import threading
import time
from time import sleep
from typing import List

from aas_python_http_client import Endpoint, ProtocolInformation
from basyx.aas.adapter.json import AASToJsonEncoder
from basyx.aas.model import SpecificAssetId, ExternalReference
from requests import ConnectTimeout

from aas.couch_db_shell_client import CouchDBShellClient
from aas.couch_db_shell_descriptor_client import CouchDBShellDescriptorClient
from aas.couch_db_submodel_client import CouchDBSubmodelClient
from aas.couch_db_submodel_descriptor_client import CouchDBSubmodelDescriptorClient
from logger.logger import set_basic_config
from model.aas_services import AasServices
from model.aas_source import AasSource
from services.aas_utils import get_base_url, get_base_url_shell_repo, encode_id, get_base_url_submodel_repo, \
    convert_dict_keys_to_camel_case

# Configure logging
set_basic_config()


class AasSourcePoller:
    log = logging.getLogger(__name__)

    def __init__(self, aas_source: AasSource):
        self.aas_source: AasSource = aas_source
        if aas_source is not None and aas_source.polling_interval_s != -1:
            self._sleep_time_in_s: int = int(self.aas_source.polling_interval_s)
        else:
            self._sleep_time_in_s: int = 60
        self._stop_polling: bool = False
        self._couchdb_shell_descriptor_client = CouchDBShellDescriptorClient(client_name=self.aas_source.name)
        self._couchdb_shell_client = CouchDBShellClient()
        self._couchdb_submodel_descriptor_client = CouchDBSubmodelDescriptorClient(client_name=self.aas_source.name)
        self._couchdb_submodel_client = CouchDBSubmodelClient()
        self._create_db_views()

        self._initiator_thread: threading.Thread = threading.Thread(target=self.do_polling)
        self._initiator_thread.start()


    def _create_db_views(self):
        db_clients = [self._couchdb_shell_descriptor_client, self._couchdb_shell_client, self._couchdb_submodel_descriptor_client, self._couchdb_submodel_client]

        for db_client in db_clients:
            db_client.add_source_name_as_view(self.aas_source.name.lower())


    def _specific_asset_id_serializer(self, obj):
        if isinstance(obj, SpecificAssetId) or isinstance(obj, ExternalReference):
            return json.loads(json.dumps(obj, cls=AASToJsonEncoder))
        raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

    def do_polling(self):
        while not self._stop_polling:
            self._log("Start")

            threads = self.create_polling_threads()
            self._start_threads(threads)
            self._join_threads(threads)

            self._log(f"Restart in {self._sleep_time_in_s} seconds...")
            sleep(self._sleep_time_in_s)
        self._log("Polling finished")

    def create_polling_threads(
            self,
            poll_descriptors: bool = True,
            poll_shells: bool = True,
            poll_submodel_descriptors: bool = True,
            poll_submodels: bool = True
    ):
        # Get shell descriptors from AAS Source, endpoint: /shell-descriptors and write submodel descriptors:
        threads = []
        if poll_descriptors and self.aas_source.shell_registry_client is not None:
            thread_descriptors = threading.Thread(target=self.poll_descriptors)
            threads.append(thread_descriptors)

        # Get shells from AAS Source, endpoint: /shells:
        if poll_shells and self.aas_source.shell_repository_client is not None:
            thread_shells = threading.Thread(target=self.poll_shells)
            threads.append(thread_shells)

        # Get submodel descriptors from AAS Source, endpoint: /submodel-descriptors:
        if poll_submodel_descriptors and isinstance(self.aas_source, AasServices) and self.aas_source.submodel_registry_client is not None:
            thread_submodel_descriptors = threading.Thread(target=self.poll_submodel_descriptors)
            threads.append(thread_submodel_descriptors)

        # Get submodels from AAS Source, endpoint: /submodels:
        if poll_submodels and self.aas_source.submodel_repository_client is not None:
            thread_submodels = threading.Thread(target=self.poll_submodels)
            threads.append(thread_submodels)

        return threads

    def _start_threads(self, threads: list[threading.Thread]):
        for thread in threads:
            thread.start()

    def _join_threads(self, threads: list[threading.Thread]):
        for thread in threads:
            thread.join()

    def poll_descriptors(self):
        shell_descriptors = self.poll_shell_descriptors()
        if shell_descriptors is None:
            return
        thread_write_shell_descriptors = threading.Thread(target=self.write_shell_descriptors, args=(shell_descriptors,))
        thread_write_submodel_descriptors = threading.Thread(target=self.write_submodel_descriptors, args=(shell_descriptors,))
        threads = [thread_write_shell_descriptors, thread_write_submodel_descriptors]
        self._start_threads(threads)
        self._join_threads(threads)

    def poll_shell_descriptors(self):
        try:
            start = time.time()
            shell_descriptors = self.aas_source.request_shell_descriptors()
            end = time.time()
            self._log(f"Polling shell descriptors took {end - start:.2f} seconds")
            self._log(f"Found {len(shell_descriptors)} shell descriptors")
            # Add local shell/submodel endpoint:
            self._add_local_endpoints(shell_descriptors)
            return shell_descriptors
        except (TimeoutError, ConnectTimeout) as e:
            self._log(f"TimeoutError: {e}", level=logging.ERROR)
            return None

    def poll_submodel_descriptors(self):
        try:
            start = time.time()
            submodel_descriptors = self.aas_source.request_submodel_descriptors()
            end = time.time()
            self._log(f"Polling submodel descriptors took {end - start:.2f} seconds")
            self._log(f"Found {len(submodel_descriptors)} submodel descriptors")
            self._couchdb_submodel_descriptor_client.save_submodel_descriptors(self.aas_source.name, submodel_descriptors)
            return submodel_descriptors
        except (TimeoutError, ConnectTimeout) as e:
            self._log(f"TimeoutError: {e}", level=logging.ERROR)
            return None

    def poll_shells(self):
        try:
            start = time.time()
            shells = self.aas_source.request_shells()
            end = time.time()
            self._log(f"Polling shells took {end - start:.2f} seconds")
            self._log(f"Found {len(shells)} shells")
            self._couchdb_shell_client.save_shells(self.aas_source.name, shells)
            return shells
        except (TimeoutError, ConnectTimeout) as e:
            self._log(f"TimeoutError: {e}", level=logging.ERROR)
            return None

    def poll_submodels(self):
        try:
            start = time.time()
            submodels = self.aas_source.request_submodels()
            end = time.time()
            elapsed_time = end - start
            self._log(f"Polling submodels took {end - start:.2f} seconds")
            self._log(f"Found {len(submodels)} submodels")
            self._couchdb_submodel_client.save_submodels(self.aas_source.name, submodels)
            return submodels
        except (TimeoutError, ConnectTimeout) as e:
            self._log(f"TimeoutError: {e}", level=logging.ERROR)
            return None

    def write_shell_descriptors(self, shell_descriptors):
        self._couchdb_shell_descriptor_client.save_shell_descriptors(self.aas_source.name, shell_descriptors)


    def write_submodel_descriptors(self, shell_descriptors: List[dict]):
        submodel_descriptors = []
        for shell_descriptor in shell_descriptors:
            try:
                submodel_descriptors.extend(shell_descriptor['submodelDescriptors'])
            except KeyError:
                continue

        self._log(f"Found {len(submodel_descriptors)} submodel descriptors")
        self._couchdb_submodel_descriptor_client.save_submodel_descriptors(self.aas_source.name, submodel_descriptors)
        return submodel_descriptors

    def stop_polling(self):
        self._log("Stop polling...")
        self._stop_polling = True
        self._initiator_thread.join()

    def _add_local_endpoints(self, shell_descriptors):
        start = time.time()
        for shell_descriptor in shell_descriptors:
            self._add_local_shell_endpoint(shell_descriptor)
            self._add_local_submodel_endpoints(shell_descriptor)
        end = time.time()
        elapsed_time = end - start
        self._log(f"Adding local endpoints to descriptors took {elapsed_time:.2f} seconds")

    def _endpoints_list_contains_local_endpoint(self, endpoints: List[dict]) -> bool:
        for endpoint in endpoints:
            if get_base_url() in endpoint['protocolInformation']['href']:
                return True
        return False

    def _add_local_shell_endpoint(self, shell_descriptor: dict):
        if self._endpoints_list_contains_local_endpoint(shell_descriptor['endpoints']):
            return

        local_shell_endpoint = convert_dict_keys_to_camel_case(Endpoint(
            interface="local",
            protocol_information=ProtocolInformation(
                href=get_base_url_shell_repo() + "/shells/" + encode_id(shell_descriptor['id'])
            )
        ).to_dict())
        shell_descriptor['endpoints'].insert(0, local_shell_endpoint)
        # return shell_descriptor.endpoints

    def _add_local_submodel_endpoints(self, shell_descriptor: dict):
        try:
            for submodel_descriptor in shell_descriptor['submodel_descriptors']:
                if self._endpoints_list_contains_local_endpoint(submodel_descriptor['endpoints']):
                    continue
                self._add_local_submodel_endpoint(submodel_descriptor)
        except KeyError:
            return

    def _add_local_submodel_endpoint(self, submodel_descriptor: dict):
        local_submodel_endpoint = convert_dict_keys_to_camel_case(Endpoint(
            interface="local",
            protocol_information=ProtocolInformation(
                href=get_base_url_submodel_repo() + "/submodels/" + encode_id(submodel_descriptor['id'])
            )
        ).to_dict())
        submodel_descriptor['endpoints'].insert(0, local_submodel_endpoint)


    def _log(self, message: str, level: int = logging.INFO):
        if level == logging.DEBUG:
            self.log.debug(f"{self.aas_source.name} | {message}")
        elif level == logging.ERROR:
            self.log.error(f"{self.aas_source.name} | {message}")
        else:
            self.log.info(f"{self.aas_source.name} | {message}")
