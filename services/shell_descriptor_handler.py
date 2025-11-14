from aas.couch_db_shell_descriptor_client import CouchDBShellDescriptorClient
from services.abstract_handler import AbstractHandler
from services.in_memory_store.in_memory_store import InMemoryStore


class ShellDescriptorHandler(AbstractHandler):

    def __init__(self):
        self.in_memory_store = None
        if self._use_in_memory_store:
            self.in_memory_store = InMemoryStore()
        self.couch_db_shell_descriptor_client = CouchDBShellDescriptorClient(client_name=self.__class__.__name__)

    def _total_count(self, aas_source_name: str) -> int:
        if aas_source_name is None:
            return self.couch_db_shell_descriptor_client.total_doc_count
        else:
            return self.couch_db_shell_descriptor_client.total_view_doc_count(aas_source_name)

    def shell_descriptors(self, aas_source_name: str, limit: int, cursor: str):
        if self.in_memory_store:
            start, end = self.get_start_end(limit, cursor)
            if aas_source_name is not None:
                base_url = self.get_base_url_by_aas_server_name(aas_source_name)
                return self.shell_descriptors_by_base_url(base_url)[start:end]
            else:
                return self.in_memory_store.shell_descriptors[start:end]
        else:
            return self.couch_db_shell_descriptor_client.get_shell_descriptors(
                aas_source_name,
                limit,
                int(cursor)
            )

    def shell_descriptor(self, identifier: str):
        for descriptor in self.shell_descriptors:
            try:
                if descriptor['id'] == identifier:
                    return descriptor
            except KeyError:
                continue
        return None

    def shell_descriptors_by_base_url(self, base_url: str):
        descriptors = []
        for descriptor in self.shell_descriptors:
            if isinstance(descriptor, dict):
                try:
                    if self.endpoints_contain_base_url(descriptor['endpoints'], base_url):
                        descriptors.append(descriptor)
                except KeyError as e:
                    continue
            else:
                if self.endpoints_contain_base_url(descriptor.endpoints, base_url):
                    descriptors.append(descriptor)
        return descriptors



    def get_shell_descriptors_by_aas_server_name(self, aas_source_name:str, limit: int, cursor: str):
        # if aas_server_name is not None:
        #     base_url = self.get_base_url_by_aas_server_name(aas_server_name)
        #     shell_descriptors = self.shell_descriptors_by_base_url(base_url)
        # else:
        shell_descriptors = self.shell_descriptors(aas_source_name, limit, cursor)

        # start, end, new_cursor = self.get_start_end_cursor(shell_descriptors, limit, cursor)

        # if limit == -1:
        #     return shell_descriptors, None
        return shell_descriptors, self.get_cursor(limit, cursor, self._total_count(aas_source_name))