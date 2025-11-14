import json
import logging
import os
import threading
import time

from dotenv import load_dotenv
from paho.mqtt.client import Client
from pycouchdb.feedreader import BaseFeedReader
from requests.exceptions import ChunkedEncodingError

from aas.couch_db_shell_client import CouchDBShellClient
from aas.couch_db_submodel_client import CouchDBSubmodelClient
from logger.logger import set_basic_config, LOG_LEVEL

load_dotenv()

# Configure logging
set_basic_config()


class MqttClient(BaseFeedReader):
    log = logging.getLogger(__name__)
    log.setLevel(logging.DEBUG)

    TOPIC_CREATE_SHELL = "aas-repository/aas-repo/shells/created"
    TOPIC_DELETE_SHELL = "aas-repository/aas-repo/shells/deleted"
    TOPIC_CREATE_SUBMODEL = "sm-repository/sm-repo/submodels/created"
    TOPIC_DELETE_SUBMODEL = "sm-repository/sm-repo/submodels/deleted"

    def __init__(self):
        self.client_id = os.getenv("MQTT_CLIENT_ID")
        self.hostname = os.getenv("MQTT_HOSTNAME")
        self.port = int(os.getenv("MQTT_PORT"))
        self._client = Client(client_id=self.client_id)

        self._shell_db_client = CouchDBShellClient()
        self._submodel_db_client = CouchDBSubmodelClient()
        self._latest_change_shells = self._shell_db_client.get_latest_change_seq()
        self._shells_feed_reader_thread: threading.Thread = threading.Thread(target=self._register_feed_reader_shells)
        self._submodels_feed_reader_thread: threading.Thread = threading.Thread(target=self._register_feed_reader_submodels)

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_publish = self._on_publish

        connect_thread = threading.Thread(target=self._connect)
        connect_thread.start()

        self._shells_feed_reader_thread.start()
        self._submodels_feed_reader_thread.start()

    def _connect(self):
        retry_timeout_in_s = 30
        while True:
            try:
                self._log("Connecting to MQTT broker...")
                self._client.connect(host=self.hostname, port=self.port)
                self._client.loop_start()
                break
            except ConnectionRefusedError:
                self._log(f"Connection refused | Retrying in {retry_timeout_in_s} seconds...", log_level=logging.WARNING)
                time.sleep(retry_timeout_in_s)

    def _register_feed_reader_shells(self):
        while True:
            try:
                self._log("Connecting to CouchDB changes feed for shells")
                db = self._shell_db_client.db
                db.changes_feed(feed_reader=self._feed_reader, since="now")
            except ChunkedEncodingError as e:
                pass


    def _register_feed_reader_submodels(self):
        while True:
            try:
                self._log("Connecting to CouchDB changes feed for submodels")
                db = self._submodel_db_client.db
                db.changes_feed(feed_reader=self._feed_reader, since="now")
            except ChunkedEncodingError as e:
                pass


    def _feed_reader(self, message, db):
        if 'seq' not in message:
            return

        if db.name == self._shell_db_client.database_name:
            doc = self._shell_db_client.get_doc(message['id'])
            if 'deleted' in message:
                topic = self.TOPIC_DELETE_SHELL
            else:
                topic = self.TOPIC_CREATE_SHELL
        elif db.name == self._submodel_db_client.database_name:
            doc = self._submodel_db_client.get_doc(message['id'])
            if 'deleted' in message:
                topic = self.TOPIC_DELETE_SUBMODEL
            else:
                topic = self.TOPIC_CREATE_SUBMODEL
        try:
            payload = json.dumps(doc['data'])
            self._client.publish(topic=topic, payload=payload)
        except KeyError:
            return

    def _on_connect(self, client, userdata, flags, rc):
        self._log("Connected to MQTT broker")

    def _on_publish(self, client, userdata, mid):
        pass

    def _on_subscribe(self, topic: str):
        # Logic to subscribe to a topic
        pass

    def _on_disconnect(self):
        self._log("Disconnected from MQTT broker")

    def _log(self, message: str, log_level: logging=logging.INFO):
        output = f"{message}"
        if log_level is logging.INFO:
            self.log.info(output)
        else:
            self.log.debug(output)