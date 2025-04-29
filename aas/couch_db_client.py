import json
import logging
import os
import threading
import time
import urllib
from abc import ABC

from basyx.aas.adapter.json import AASToJsonEncoder
from basyx.aas.model import SpecificAssetId, ExternalReference
from dotenv import load_dotenv
from pycouchdb import Server
from pycouchdb.client import Database
from pycouchdb.exceptions import Conflict

from services.aas_utils import convert_dict_keys_to_camel_case

load_dotenv()


def serializer(obj):
    if isinstance(obj, SpecificAssetId) or isinstance(obj, ExternalReference):
        return json.loads(json.dumps(obj, cls=AASToJsonEncoder))
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

class CouchDBClient(ABC):
    log = logging.getLogger(__name__)

    def __init__(self, database_name: str = None, client_name: str = "default-client"):
        self.client_name: str = client_name
        self.database_name: str = database_name
        self._host: str = os.getenv('COUCHDB_HOST')
        self._port: str = os.getenv('COUCHDB_PORT')
        self._username: str = os.getenv('COUCHDB_USERNAME')
        self._password: str = os.getenv('COUCHDB_PASSWORD')
        self._server: Server = Server(base_url=self.base_url_with_creds, authmethod="basic")
        self._db: Database = None
        self._max_count_bulk_save = 25000

    @property
    def db(self):
        if self._db is None:
            self._db = self._server.database(self.database_name)
        return self._db


    def _create_database(self):
        """
        Create the database if it does not exist.
        """
        self.create_db()


    @property
    def base_url(self) -> str:
        return f"http://{self._host}:{self._port}"

    @property
    def base_url_with_creds(self) -> str:
        return f"http://{self._username}:{self._password}@{self._host}:{self._port}"

    def _change_list(self):
        return self.db.changes_list()

    def _latest_change_seq(self):
        change_list = self._change_list()
        if change_list:
            if len(change_list[1]) == 0:
                return None
            else:
                return change_list[0]

    def get_change_list(self):
        return self._change_list()


    def get_latest_change_seq(self):
        return self._latest_change_seq()

    def create_db(self):
        try:
            self._server.create(self.database_name)
        except Exception as e:
            pass
            # print(f"Database \"{self._database_name}\" exists already | Skip create: {e}")

    def get_all_docs(self):
        return self.db.all(as_list=True)

    def get_doc(self, doc_id: str):
        doc_id_quoted = urllib.parse.quote(doc_id, safe='')
        try:
            return self.db.get(doc_id_quoted)
        except Exception as e:
            # print(f"Error getting document \"{doc_id}\" from \"{self._database_name}\": {e}")
            return None

    def get_doc_rev(self, doc_id: str):
        doc = self.get_doc(doc_id=doc_id)
        return doc['_rev'] if doc else None

    def save_doc(self, doc: dict):
        try:
            self.db.save(doc)
        except Conflict:
            pass
        except Exception as e:
            print(f"Error saving document to \"{self.database_name}\": {e}")
            return None

    def save_docs(self, docs: list[dict]):
        try:
            self.db.save_bulk(docs=docs, transaction=False)

        except Conflict as e:
            pass
        except Exception as e:
            print(f"Error saving documents to \"{self.database_name}\": {e}")


    def save_entities(self, entities: list[object]):
        entity_splits = [entities[i:i + self._max_count_bulk_save] for i in range(0, len(entities), self._max_count_bulk_save)]

        if len(entity_splits) > 1:
            self._run_save_entity_threads(entity_splits)
        elif len(entity_splits) == 1:
            self._save_entity_list(entity_splits[0])


    def _run_save_entity_threads(self, entity_splits: list[list[object]]):
        threads = []
        total_entity_count = sum(len(split) for split in entity_splits)

        for entity_split in entity_splits:
            thread = threading.Thread(target=self._save_entity_list, args=(entity_split,))
            threads.append(thread)

        start = time.time()
        for thread in threads:
            thread.start()

        self._log(f"Started {len(threads)} thread(s) to save {total_entity_count} entities into {self.database_name}.")

        for thread in threads:
            thread.join()
        end = time.time()
        elapsed = end - start

        self._log(f"Finished {len(threads)} thread(s) to save {total_entity_count} entititis into {self.database_name} in {elapsed:.2f} seconds.")


    def _save_entity_list(self, entity_list: list[object]):
        docs = []
        for entity in entity_list:
            ## Diff with doc from DB
            if isinstance(entity, dict):
                id = entity['id']
            else:
                id = entity.id

            doc = self.get_doc(id)
            current_entity = None if doc is None else doc['data']
            is_equal = None
            # Check if current entity is equal to new entity:
            if current_entity is not None:
                is_equal = self._equals(current_entity, entity)
                # continue if current entity is equal to the new entity
                if is_equal:
                    continue
            # Prepare the payload for saving
            if isinstance(entity, dict):
                data = entity
            else:
                data =  convert_dict_keys_to_camel_case(
                    json.loads(json.dumps(entity.to_dict(), default=serializer))
                )
            payload = {
                "_id": id,
                "data": data
            }

            # add rev to payload to update if new entity diffs from current entity
            if is_equal is not None and not is_equal:
                payload['_rev'] = self.get_doc_rev(id)
            docs.append(payload)

        self.save_docs(docs=docs)


    def delete_doc(self, doc_id: str):
        doc_id_quoted = urllib.parse.quote(doc_id, safe='')
        self.db.delete(doc_id_quoted)


    def _equals(self, a: object, b: object) -> bool:
        """
        Check if two objects are equal.
        """
        if not isinstance(a, dict):
            a_dict = json.loads(json.dumps(a.to_dict(), default=serializer))
        else:
            a_dict = a

        if not isinstance(b, dict):
            b_dict = json.loads(json.dumps(b.to_dict(), default=serializer))
        else:
            b_dict = b


        dict_equal = self._dict_equal(a_dict, b_dict)

        return dict_equal

    def _dict_equal(self, a: dict, b: dict) -> bool:
        """
        Check if two dictionaries are equal.
        """
        return a == b


    def _log(self, message: str, log_level: logging=logging.INFO):
        output = f"{self.__class__.__name__} | {self.client_name} | {message}"
        if log_level is logging.INFO:
            self.log.info(output)
        else:
            self.log.debug(output)
