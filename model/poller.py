import os
from typing import List

from dotenv import load_dotenv

from model.aas_source import AasSource
from model.app_config import load_config
from model.mqtt_client import MqttClient
from services.aas_source_poller import AasSourcePoller

load_dotenv()


class Poller:
    def __init__(self):
        if self._mqtt_enabled:
            self._mqtt_client = MqttClient()
        self._aas_source_poller: List[AasSourcePoller] = []

        self._watch_config()

    @property
    def _mqtt_enabled(self) -> bool:
        return os.getenv("MQTT_ENABLED", 'False').lower() == 'true'

    @property
    def _polling_white_list(self):
        polling_white_list_env = os.getenv("POLLING_WHITE_LIST")
        return set(polling_white_list_env.split(",")) if len(polling_white_list_env) > 0 else []


    def _watch_config(self):
        while True:
            config = load_config()
            for aas_source in config.aas_servers:
                self._start_aas_source_poller(aas_source)

            self._stop_aas_source_poller()

    def _start_aas_source_poller(self, aas_source: AasSource):
        if len(self._polling_white_list) > 0:
            if aas_source.name.lower() not in (s.lower() for s in self._polling_white_list):
                return

        if self._is_aas_source_poller_running(aas_source):
            return

        self._aas_source_poller.append(AasSourcePoller(aas_source))


    def _stop_aas_source_poller(self):
        config = load_config()

        for poller in self._aas_source_poller:
            if poller.aas_source.name.lower() not in (source.name.lower() for source in config.aas_servers):
                poller.stop_polling()
                self._aas_source_poller.remove(poller)


    def _is_aas_source_poller_running(self, aas_source: AasSource) -> bool:
        for poller in self._aas_source_poller:
            if poller.aas_source.name == aas_source.name:
                return True
        return False


