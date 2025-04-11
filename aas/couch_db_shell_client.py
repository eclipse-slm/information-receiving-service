import logging
from typing import List, Dict

from aas.couch_db_client import CouchDBClient
from logger.logger import set_basic_config

# Configure logging
set_basic_config()

class CouchDBShellClient(CouchDBClient):
    log = logging.getLogger(__name__)

    def __init__(self):
        super().__init__(database_name="shells")
        self._create_database()

    def get_all_shells(self) -> List[Dict]:
        docs = self.get_all_docs()
        shells = []
        for doc in docs:
            shells.append(
                # json.loads(json.dumps(doc['doc']['data']), cls=AASFromJsonDecoder)
                doc['doc']['data']
            )
        return shells

    def save_shells(self, shells: List[Dict]):
        self.save_entities(shells)


    def _log(self, message: str):
        self.log.info(f"{self.__class__.__name__} | {message}")
