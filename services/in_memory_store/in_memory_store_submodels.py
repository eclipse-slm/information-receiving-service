import threading
from typing import List

from aas.couch_db_submodel_client import CouchDBSubmodelClient
from services.in_memory_store.in_memory_store_abstract import AbstractInMemoryStore


class InMemoryStoreSubmodels(AbstractInMemoryStore):
    def __init__(self):
        super().__init__(couch_db_client=CouchDBSubmodelClient())
        self._feed_reader_thread: threading.Thread = threading.Thread(target=self._register_feed_reader)

        self._init()


    def _init(self):
        self._get_submodels()
        self._feed_reader_thread.start()


    def _get_submodels(self):
        self._log("Start loading items from CouchDB into Memory")
        self.store = self._db_client.get_all_submodels()
        self._log(f"Finished loading {len(self.store)} items from CouchDB into Memory.")


    def submodel(self, identifier: str):
        for submodel in self.store:
            if submodel['id'] == identifier:
                return submodel
        return None

    def get_store_filtered(self, ids: List[str]):
        filtered_store = []
        for submodel in self.store:
            try:
                if submodel['id'] in ids:
                    filtered_store.append(submodel)
            except KeyError as e:
                continue
        return filtered_store