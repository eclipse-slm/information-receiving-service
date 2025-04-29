import threading
from typing import List

from aas_python_http_client import AssetAdministrationShellDescriptor, ApiClient

from aas.couch_db_shell_descriptor_client import CouchDBShellDescriptorClient
from services.aas_utils import convert_dict_keys_to_camel_case
from services.in_memory_store import in_memory_store_abstract
from services.in_memory_store.in_memory_store_abstract import AbstractInMemoryStore

# api_client = ApiClient()

class InMemoryStoreShellDescriptors(AbstractInMemoryStore):
    def __init__(self):
        super().__init__(couch_db_client=CouchDBShellDescriptorClient(client_name="InMemoryStoreShellDescriptors"))
        self._feed_reader_thread: threading.Thread = threading.Thread(target=self._register_feed_reader)

        self._init()

    def _init(self):
        self._get_shell_descriptors()
        self._feed_reader_thread.start()

    def _get_shell_descriptors(self):
        self._log("Start loading items from CouchDB into Memory")
        self.store = self._db_client.get_all_shell_descriptors()
        # descriptors_list = api_client.sanitize_for_serialization(descriptors)
        # self.store = convert_dict_keys_to_camel_case(descriptors_list)
        self._log(f"Finished loading {len(self.store)} items from CouchDB into Memory.")


    def shell_descriptor(self, identifier: str) -> dict:
        """
        Get a shell descriptor by its identifier.

        Args:
            identifier (str): The identifier of the shell descriptor.

        Returns:
            AssetAdministrationShellDescriptor: The shell descriptor.
        """
        for descriptor in self.store:
            try:
                if descriptor['id'] == identifier:
                    return descriptor
            except KeyError as e:
                continue
        return None

    def get_store_filtered(self, base_url: str) -> List[dict]:
        descriptors = []
        for descriptor in self.store:
            if isinstance(descriptor, dict):
                try:
                    if in_memory_store_abstract.endpoints_contain_base_url(descriptor['endpoints'], base_url):
                        descriptors.append(descriptor)
                except KeyError as e:
                    continue
            else:
                if in_memory_store_abstract.endpoints_contain_base_url(descriptor.endpoints, base_url):
                    descriptors.append(descriptor)
        return descriptors