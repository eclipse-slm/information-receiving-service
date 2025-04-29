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
            submodels.append(
                doc['doc']['data']
            )
        return submodels


    def get_submodel(self, identifier: str) -> Dict:
        doc = self.get_doc(identifier)
        return doc['data']


    def save_submodels(self, submodels: List[Dict]):
        docs = []
        counter = 0
        for submodel in submodels:
            counter += 1
            try:
                data = json.loads(json.dumps(submodel, cls=AASToJsonEncoder))
                payload = {
                    "_id": submodel['id'],
                    "data": data
                }
                docs.append(payload)
            except Exception as e:
                self._log(f"Failed to save submodel | {e}")
            if counter == 10000:
                self.save_docs(docs=docs)
                counter = 0
                docs = []

        self.save_docs(docs=docs)

    def delete_submodel(self, sm_identifier: str):
        self.delete_doc(doc_id=sm_identifier)


    def _log(self, message: str):
        self.log.info(f"{self.__class__.__name__} | {message}")