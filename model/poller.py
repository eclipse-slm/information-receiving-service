import os

from dotenv import load_dotenv

from model.app_config import load_config
from services.aasx_server_poller import AasxServerPoller

load_dotenv()
config = load_config()


class Poller:
    def __init__(self):
        for aas_server in config.aas_servers:
            if len(self._polling_white_list) > 0:
                if aas_server.name.lower() not in (s.lower() for s in self._polling_white_list):
                    continue
            AasxServerPoller(aas_server)

    @property
    def _polling_white_list(self):
        polling_white_list_env = os.getenv("POLLING_WHITE_LIST")
        return set(polling_white_list_env.split(",")) if len(polling_white_list_env) > 0 else []