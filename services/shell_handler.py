from typing import List

from aas.couch_db_shell_client import CouchDBShellClient
from services.abstract_handler import AbstractHandler
from services.in_memory_store.in_memory_store import InMemoryStore
from services.shell_descriptor_handler import ShellDescriptorHandler


class ShellHandler(AbstractHandler):
    def __init__(self):
        self.in_memory_store = None
        if self._use_in_memory_store:
            self.in_memory_store = InMemoryStore()
        self.shell_descriptor_handler = ShellDescriptorHandler()
        self.couch_db_shell_client = CouchDBShellClient()

    def _total_count(self, aas_source_name: str) -> int:
        if aas_source_name is None:
            return self.couch_db_shell_client.total_doc_count
        else:
            return self.couch_db_shell_client.total_view_doc_count(aas_source_name)


    def shells(self, aas_source_name: str = None, limit: int = -1, cursor: str = "0"):
        if self.in_memory_store:
            start, end = self.get_start_end(limit, cursor)
            if aas_source_name is None:
                return self.in_memory_store.shells[start:end]
            else:
                descriptors_by_source_name, cursor = self.shell_descriptor_handler.get_shell_descriptors_by_aas_server_name(aas_source_name, -1, "0")
                shell_descriptor_ids_by_source_name = [descriptor['id'] for descriptor in descriptors_by_source_name]
                return self.shells_by_ids(shell_descriptor_ids_by_source_name)[start:end]
        else:
            return self.couch_db_shell_client.get_shells(aas_source_name, limit, int(cursor))


    def shell(self, identifier: str):
        for shell in self.shells():
            try:
                if shell['id'] == identifier:
                    return shell
            except KeyError:
                continue
        return None

    def shells_by_ids(self, ids: List[str]):
        filtered_shells = []
        for shell in self.shells():
            try:
                if shell['id'] in ids:
                    filtered_shells.append(shell)
            except KeyError:
                continue
        return filtered_shells


    def get_shells_by_aas_server_name(self, aas_source_name:str, limit: int, cursor: str):
        shells = self.shells(aas_source_name, limit, cursor)
        return shells, self.get_cursor(limit, cursor, self._total_count(aas_source_name))
        # if aas_server_name is None:
        #     shells = self.shells
        # else:
        #     descriptors_by_server_name, cursor = self.shell_descriptor_handler.get_shell_descriptors_by_aas_server_name(aas_server_name, -1, "0")
        #     shell_descriptor_ids_by_server_name = [descriptor['id'] for descriptor in descriptors_by_server_name]
        #     shells = self.shells_by_ids(shell_descriptor_ids_by_server_name)
        #
        # start, end, new_cursor = self.get_start_end_cursor(shells, limit, cursor)
        #
        # return shells[start:end], new_cursor

    def get_shell_ids_by_asset_id(self, asset_id: str):
        """
        Get the shell ids linked to a specific Asset identifier.

        Args:
            asset_id (str): The identifier of the Asset.

        Returns:
            List[str]: A list of shell ids linked to the Asset identifier.
        """
        shell_ids = []
        for shell in self.shells:
            if shell['assetInformation']['globalAssetId'] == asset_id:
                shell_ids.append(shell['id'])
        return shell_ids