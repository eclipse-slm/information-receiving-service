import threading
from typing import List

from aas.couch_db_shell_client import CouchDBShellClient
from services.in_memory_store.in_memory_store_abstract import AbstractInMemoryStore


class InMemoryStoreShells(AbstractInMemoryStore):
    def __init__(self):
        super().__init__(couch_db_client=CouchDBShellClient())
        self._feed_reader_thread: threading.Thread = threading.Thread(target=self._register_feed_reader)

        self._init()


    def _init(self):
        self._get_shells()
        self._feed_reader_thread.start()


    def _get_shells(self):
        self._log("Start loading items from CouchDB into Memory")
        self.store = self._db_client.get_all_shells()
        self._log(f"Finished loading {len(self.store)} items from CouchDB into Memory.")


    def shell(self, identifier: str):
        for shell in self.store:
            if shell['id'] == identifier:
                return shell
        return None


    def get_store_filtered(self, ids: List[str]):
        filtered_store = []
        for shell in self.store:
            try:
                if shell['id'] in ids:
                    filtered_store.append(shell)
            except KeyError as e:
                continue
        return filtered_store

