import json
import logging
import threading
import time
from time import sleep

from aas_python_http_client import Endpoint, ProtocolInformation
from basyx.aas.adapter.json import AASToJsonEncoder
from basyx.aas.model import SpecificAssetId, ExternalReference
from requests import ConnectTimeout

from aas.couch_db_shell_client import CouchDBShellClient
from aas.couch_db_shell_descriptor_client import CouchDBShellDescriptorClient
from aas.couch_db_submodel_client import CouchDBSubmodelClient
from aas.couch_db_submodel_descriptor_client import CouchDBSubmodelDescriptorClient
from logger.logger import set_basic_config
from model.aasx_server import AasxServer
from services.aas_utils import get_base_url, get_base_url_shell_repo, encode_id, get_base_url_submodel_repo

# Configure logging
set_basic_config()


class AasxServerPoller:
    log = logging.getLogger(__name__)

    def __init__(self, aasx_server: AasxServer):
        self.aasx_server: AasxServer = aasx_server
        # self._aas_obj_store: CrawlerCouchDBObjectStore = aas_obj_store
        self._sleep_time_in_s: int = 60
        self._stop_polling: bool = False
        self._couchdb_shell_descriptor_client = CouchDBShellDescriptorClient(client_name=self.aasx_server.name)
        self._couchdb_shell_client = CouchDBShellClient()
        self._couchdb_submodel_descriptor_client = CouchDBSubmodelDescriptorClient(client_name=self.aasx_server.name)
        self._couchdb_submodel_client = CouchDBSubmodelClient()
        self._polling_thread: threading.Thread = threading.Thread(target=self.do_polling)

        self._polling_thread.start()

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

    def create_polling_threads(self, poll_descriptors: bool = True, poll_shells: bool = True, poll_submodels: bool = False):
        # Get shell descriptors from AASX Server /shell-descriptors and write submodel descriptors:
        threads = []
        if poll_descriptors:
            thread_descriptors = threading.Thread(target=self.poll_descriptors)
            threads.append(thread_descriptors)

        # Get shells from AASX Server /shells:
        if poll_shells:
            thread_shells = threading.Thread(target=self.poll_shells)
            threads.append(thread_shells)

        # Get submodels from AASX Server /submodels:
        if poll_submodels:
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
            shell_descriptors = self.aasx_server.request_shell_descriptors()
            end = time.time()
            self._log(f"Polling shell descriptors took {end - start:.2f} seconds")
            self._log(f"Found {len(shell_descriptors)} shell descriptors")
            # Add local shell/submodel endpoint:
            self._add_local_endpoints(shell_descriptors)
            return shell_descriptors
        except (TimeoutError, ConnectTimeout) as e:
            self._log(f"TimeoutError: {e}", level=logging.ERROR)
            return None

    def poll_shells(self):
        try:
            start = time.time()
            shells = self.aasx_server.request_shells()
            end = time.time()
            self._log(f"Polling shells took {end - start:.2f} seconds")
            self._log(f"Found {len(shells)} shells")
            self._couchdb_shell_client.save_shells(shells)
            return shells
        except (TimeoutError, ConnectTimeout) as e:
            self._log(f"TimeoutError: {e}", level=logging.ERROR)
            return None

    def poll_submodels(self):
        try:
            start = time.time()
            submodels = self.aasx_server.request_submodels()
            end = time.time()
            elapsed_time = end - start
            self._log(f"Polling submodels took {end - start:.2f} seconds")
            self._log(f"Found {len(submodels)} submodels")
            self._couchdb_submodel_client.save_submodels(submodels)
            return submodels
        except (TimeoutError, ConnectTimeout) as e:
            self._log(f"TimeoutError: {e}", level=logging.ERROR)
            return None

    def write_shell_descriptors(self, shell_descriptors):
        self._couchdb_shell_descriptor_client.save_shell_descriptors(shell_descriptors)


    def write_submodel_descriptors(self, shell_descriptors):
        submodel_descriptors = []
        for shell_descriptor in shell_descriptors:
            if shell_descriptor.submodel_descriptors is not None:
                submodel_descriptors.extend(shell_descriptor.submodel_descriptors)

        self._log(f"Found {len(submodel_descriptors)} submodel descriptors")
        self._couchdb_submodel_descriptor_client.save_submodel_descriptors(submodel_descriptors)
        return submodel_descriptors

    def stop_polling(self):
        self._stop_polling = True
        self._polling_thread.join()

    def _add_local_endpoints(self, shell_descriptors):
        start = time.time()
        for shell_descriptor in shell_descriptors:
            self._add_local_shell_endpoint(shell_descriptor)
            self._add_local_submodel_endpoints(shell_descriptor)
        end = time.time()
        elapsed_time = end - start
        self._log(f"Adding local endpoints to descriptors took {elapsed_time:.2f} seconds")

    def _endpoints_list_contains_local_endpoint(self, endpoints) -> bool:
        for endpoint in endpoints:
            if get_base_url() in endpoint.protocol_information.href :
                return True
        return False

    def _add_local_shell_endpoint(self, shell_descriptor):
        if self._endpoints_list_contains_local_endpoint(shell_descriptor.endpoints):
            return

        local_shell_endpoint = Endpoint(
            interface="local",
            protocol_information=ProtocolInformation(
                href=get_base_url_shell_repo() + "/shells/" + encode_id(shell_descriptor.id)
            )
        )
        shell_descriptor.endpoints.insert(0, local_shell_endpoint)
        # return shell_descriptor.endpoints

    def _add_local_submodel_endpoints(self, shell_descriptor):
        for submodel_descriptor in shell_descriptor.submodel_descriptors:
            if self._endpoints_list_contains_local_endpoint(submodel_descriptor.endpoints):
                continue
            self._add_local_submodel_endpoint(submodel_descriptor)

    def _add_local_submodel_endpoint(self, submodel_descriptor):
        local_submodel_endpoint = Endpoint(
            interface="local",
            protocol_information=ProtocolInformation(
                href=get_base_url_submodel_repo() + "/submodels/" + encode_id(submodel_descriptor.id)
            )
        )
        submodel_descriptor.endpoints.insert(0, local_submodel_endpoint)


    def _log(self, message: str, level: int = logging.INFO):
        if level == logging.DEBUG:
            self.log.debug(f"{self.aasx_server.name} | {message}")
        elif level == logging.ERROR:
            self.log.error(f"{self.aasx_server.name} | {message}")
        else:
            self.log.info(f"{self.aasx_server.name} | {message}")
