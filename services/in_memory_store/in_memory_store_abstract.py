import logging
from abc import ABC

from pycouchdb.feedreader import BaseFeedReader
from requests.exceptions import ChunkedEncodingError

from aas.couch_db_client import CouchDBClient
from logger.logger import set_basic_config

# Configure logging
set_basic_config()

def endpoints_contain_base_url(endpoints, base_url):
    for endpoint in endpoints:
        if isinstance(endpoint, dict):
            if base_url in endpoint['protocolInformation']['href']:
                return True
        else:
            if base_url in endpoint.protocol_information.href:
                return True
    return False

class AbstractInMemoryStore(ABC, BaseFeedReader):
    log = logging.getLogger(__name__)

    def __init__(self, couch_db_client: CouchDBClient):
        self.store = []
        self._db_client: CouchDBClient = couch_db_client
        self._latest_change: str = self._db_client.get_latest_change_seq()


    @classmethod
    def create_store(cls):
        return cls()


    @property
    def _since_feed(self):
        return "now" if self._latest_change is None else self._latest_change


    def _register_feed_reader(self):
        self._log("Connecting to couchDB changes feed")
        while True:
            try:
                db = self._db_client.db
                db.changes_feed(feed_reader=self, since=self._since_feed)
            except ChunkedEncodingError as e:
                pass
            self._log("Reconnecting to couchDB feed", logging.DEBUG)


    def on_message(self, message):
        if 'seq' not in message:
            return

        self._latest_change = message['seq']

        if 'deleted' in message:
            self._remove_item_from_store(message['id'])
        else:
            self._add_item_to_store(message['id'])


    def on_close(self):
        self._log("Feed connection closed")


    def on_heartbeat(self):
        pass

    def _add_item_to_store(self, identifier):
        """
        Add an item to the store.
        """
        item = self._db_client.get_doc(identifier)
        if item:
            self._remove_item_from_store(identifier)
            self.store.append(
                item['data']
                # convert_dict_keys_to_camel_case(item['data'])
            )
            # print(f"Finished adding shell descriptor with id {id} to memory.")
        else:
            self._log(f"Item with id {identifier} not found in CouchDB.")


    def _remove_item_from_store(self, identifier):
        self.store = [item for item in self.store if item['id'] != identifier]


    def _log(self, message: str, log_level: logging=logging.INFO):
        output = f"{self.__class__.__name__} | {message}"
        if log_level is logging.INFO:
            self.log.info(output)
        else:
            self.log.debug(output)
