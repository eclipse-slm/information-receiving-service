import os
from abc import ABC
from typing import List

from model.app_config import load_config


class AbstractHandler(ABC):

    @property
    def _use_in_memory_store(self):
        return os.getenv("USE_IN_MEMORY_STORE").lower() == "true"


    def get_base_url_by_aas_server_name(self, aas_server_name:str) -> str:
        aasx_servers = load_config().aas_servers
        for aasx_server in aasx_servers:
            if aasx_server.name.lower() == aas_server_name.lower():
                return aasx_server.url
        return None


    def endpoints_contain_base_url(self, endpoints, base_url) -> bool:
        for endpoint in endpoints:
            if isinstance(endpoint, dict):
                if base_url in endpoint['protocolInformation']['href']:
                    return True
            else:
                if base_url in endpoint.protocol_information.href:
                    return True
        return False


    def get_start_end_cursor(self, aas_objects: List[dict], limit: int, cursor: str):
        start = int(cursor) if cursor is not None else 0
        end = start + limit
        new_cursor = str(end) if end < len(aas_objects) else None
        return start, end, new_cursor
