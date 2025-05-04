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
            try:
                shells.append(
                    # json.loads(json.dumps(doc['doc']['data']), cls=AASFromJsonDecoder)
                    doc['doc']['data']
                )
            except KeyError:
                continue
        return shells

    def get_shells(self, source_name: str, limit: int, cursor: int) -> List[Dict]:
        shells = []
        if source_name is None:
            rows = self.get_all_docs(limit=limit, skip=cursor)
        else:
            rows = self.get_view_docs(source_name, limit, cursor)

        for row in rows:
            try:
                shells.append(row['doc']['data'])
            except KeyError:
                continue
        return shells

    def save_shells(self, source_name: str, shells: List[dict]):
        self.save_entities(source_name, shells)

    def delete_shell(self, aas_identifier: str):
        self.delete_doc(doc_id=aas_identifier)


    def _log(self, message: str):
        self.log.info(f"{self.__class__.__name__} | {message}")
