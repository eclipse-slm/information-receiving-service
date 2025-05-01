# Configure logging
import json
import logging
from typing import List, Dict

from basyx.aas.adapter.json import AASToJsonEncoder

from aas.couch_db_client import CouchDBClient
from logger.logger import set_basic_config

set_basic_config()

class CouchDBSubmodelClient(CouchDBClient):
    log = logging.getLogger(__name__)

    def __init__(self):
        super().__init__(database_name="submodels")
        self._create_database()

    def get_all_submodels(self) -> List[Dict]:
        docs = self.get_all_docs()
        submodels = []
        for doc in docs:
            try:
                submodels.append(
                    doc['doc']['data']
                )
            except KeyError:
                continue
        return submodels


    def get_submodel(self, identifier: str) -> Dict:
        doc = self.get_doc(identifier)
        if doc:
            return doc['data']
        else:
            return None


    def save_submodels(self, source_name: str, submodels: List[dict]):
        count_limit = 10000
        entities = []
        counter = 0

        if len(submodels) >count_limit:
            for submodel in submodels:
                counter += 1
                entities.append(submodel)
                if counter == count_limit:
                    self.save_entities(source_name, entities)
                    counter = 0
                    entities = []
        else:
            entities = submodels

        self.save_entities(source_name, entities)

    def delete_submodel(self, sm_identifier: str):
        self.delete_doc(doc_id=sm_identifier)


    def _log(self, message: str):
        self.log.info(f"{self.__class__.__name__} | {message}")