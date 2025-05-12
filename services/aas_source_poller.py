import json
import logging
import os
import threading
import time
from time import sleep
from typing import List

from aas_python_http_client import Endpoint, ProtocolInformation, AssetAdministrationShellDescriptor
from aas_python_http_client.rest import ApiException
from basyx.aas.adapter.json import AASToJsonEncoder
from basyx.aas.model import SpecificAssetId, ExternalReference
from requests import ConnectTimeout

from aas.couch_db_shell_client import CouchDBShellClient
from aas.couch_db_shell_descriptor_client import CouchDBShellDescriptorClient
from aas.couch_db_submodel_client import CouchDBSubmodelClient
from aas.couch_db_submodel_descriptor_client import CouchDBSubmodelDescriptorClient
from aas.shell_registry_client import ShellRegistryClient
from aas.submodel_registry_client import SubmodelRegistryClient
from logger.logger import set_basic_config
from model.aas_services import AasServices
from model.aas_source import AasSource
from services.aas_utils import get_base_url, get_base_url_shell_repo, encode_id, get_base_url_submodel_repo, \
    convert_dict_keys_to_camel_case, convert_submodel_to_submodel_descriptor, convert_shell_to_shell_descriptor, \
    api_client

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
        self._local_shell_registry_client = ShellRegistryClient()
        self._local_submodel_registry_client = SubmodelRegistryClient()
        self._couchdb_shell_descriptor_client = CouchDBShellDescriptorClient(client_name=self.aas_source.name)
        self._couchdb_shell_client = CouchDBShellClient()
        self._couchdb_submodel_descriptor_client = CouchDBSubmodelDescriptorClient(client_name=self.aas_source.name)
        self._couchdb_submodel_client = CouchDBSubmodelClient()
        self._create_db_views()

        self._initiator_thread: threading.Thread = threading.Thread(target=self.do_polling)
        self._initiator_thread.start()

    @property
    def _poll_shell_descriptors(self):
        return os.getenv("POLL_SHELL_DESCRIPTORS", 'True').lower() == 'true'

    @property
    def _poll_shells(self):
        return os.getenv("POLL_SHELLS", 'True').lower() == 'true'

    @property
    def _poll_submodel_descriptors(self):
        return os.getenv("POLL_SUBMODEL_DESCRIPTORS", 'True').lower() == 'true'

    @property
    def _poll_submodels(self):
        return os.getenv("POLL_SUBMODELS", 'True').lower() == 'true'

    @property
    def _register_shells(self):
        return os.getenv("REGISTER_SHELLS", 'false').lower() == 'true'

    @property
    def _register_submodels(self):
        return os.getenv("REGISTER_SUBMODELS", 'false').lower() == 'true'

    @property
    def _local_shell_registry_base_url(self):
        return os.getenv("AAS_SHELL_REGISTRY_HOST")

    @property
    def _local_submodel_registry_base_url(self):
        return os.getenv("AAS_SUBMODEL_REGISTRY_HOST")


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

            threads = self.create_polling_threads(
                self._poll_shell_descriptors,
                self._poll_shells,
                self._poll_submodel_descriptors,
                self._poll_submodels
            )
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
            if self._register_shells:
                self._register_shells_in_local_registry(shells)
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
            self._log(f"Polling submodels took {elapsed_time:.2f} seconds")
            self._log(f"Found {len(submodels)} submodels")
            self._couchdb_submodel_client.save_submodels(self.aas_source.name, submodels)
            if self._register_submodels:
                self._register_submodels_in_local_registry(submodels)
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

    def _register_shells_in_local_registry(self, shells: List[dict]):
        ## Convert Shells into Shell Descriptors
        shell_descriptors = []
        for shell in shells:
            shell_descriptor = convert_shell_to_shell_descriptor(shell)
            self._add_local_shell_endpoint(shell_descriptor)
            ## Add local submodel endpoints:
            self._add_submodel_descriptors_to_local_shell_descriptor(shell, shell_descriptor)

            shell_descriptors.append(shell_descriptor)

        ## Send Shell Descriptors to local registry
        for sd in shell_descriptors:
            try:
                self._local_shell_registry_client.post_asset_administration_shell_descriptor(sd)
            except ApiException as e:
                if e.status == 409:
                    self._local_shell_registry_client.put_asset_administration_shell_descriptor_by_id(sd, encode_id(sd['id']))
                else:
                    self._log(f"Error registering shell descriptor: {e}", level=logging.ERROR)

    def _add_submodel_descriptors_to_local_shell_descriptor(self, shell: dict, shell_descriptor: dict):
        submodel_descriptors = []
        submodels = []
        try:
            submodels = shell['submodels']
        except KeyError:
            self._log(f"Shell {shell['id']} has no property with the name \"submodels\"", level=logging.WARN)
        for submodel in submodels:
            submodel_id = submodel['keys'][0]['value']
            kwargs = {
                'id': submodel_id,
                'endpoints': [
                    {
                        'interface': 'local',
                        'protocolInformation': {
                            'href': get_base_url_submodel_repo() + "/submodels/" + encode_id(submodel_id)
                        }
                    }
                ]
            }
            sd = AssetAdministrationShellDescriptor(**kwargs)
            submodel_descriptors.append(api_client.sanitize_for_serialization(sd))

        shell_descriptor['submodelDescriptors'] = submodel_descriptors


    def _register_submodels_in_local_registry(self, submodels: List[dict]):
        ## Convert Submodel into Submodel Descriptors
        submodel_descriptors = []
        for submodel in submodels:
            try:
                sd = convert_submodel_to_submodel_descriptor(submodel)
            except ValueError:
                self._log(f"Error converting submodel to descriptor | submodel_id = {submodel['id']}", level=logging.ERROR)
                continue
            self._add_local_submodel_endpoint(sd)
            submodel_descriptors.append(sd)

        ## Send Submodel Descriptors to local registry
        for sd in submodel_descriptors:
            try:
                self._local_submodel_registry_client.post_submodel_descriptor(sd)
            except ApiException as e:
                if e.status == 409:
                    self._local_submodel_registry_client.put_submodel_descriptor_by_id(sd, encode_id(sd['id']))
                else:
                    self._log(f"Error registering submodel descriptor: {e}", level=logging.ERROR)

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
        log_msg = f"{self.aas_source.name} | {message}"

        if level == logging.DEBUG:
            self.log.debug(log_msg)
        elif level == logging.ERROR:
            self.log.error(log_msg)
        elif level == logging.WARN or level == logging.WARNING:
            self.log.warning(log_msg)
        else:
            self.log.info(log_msg)
