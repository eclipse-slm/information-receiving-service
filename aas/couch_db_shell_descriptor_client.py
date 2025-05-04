import json
import urllib
from typing import List

from aas_python_http_client import AssetAdministrationShellDescriptor

from aas import couch_db_client
from aas.couch_db_client import CouchDBClient


class CouchDBShellDescriptorClient(CouchDBClient):
    def __init__(self, client_name: str):
        super().__init__(database_name="shell_descriptors", client_name=client_name)
        self._create_database()

    def save_shell_descriptors(self, source_name: str, descriptors: list[AssetAdministrationShellDescriptor]):
        self.save_entities(source_name, descriptors)

    def save_shell_descriptor(self, descriptor: AssetAdministrationShellDescriptor):
        data = json.loads(json.dumps(descriptor.to_dict(), default=couch_db_client.serializer))
        payload = {
            "_id": urllib.parse.quote(descriptor.id, safe=''),
            "data": data
        }
        self.save_doc(doc=payload)

    def get_shell_descriptor(self, aas_identifier: str) -> AssetAdministrationShellDescriptor:
        doc = self.get_doc(doc_id=aas_identifier)

        if doc is None:
            return None

        return AssetAdministrationShellDescriptor(**doc['data'])

    def get_all_shell_descriptors(self, get_raw: bool = False) -> List[dict]:
        docs = self.get_all_docs()

        for doc in docs:
            if "_design" in doc['id']:
                docs.remove(doc)
                break

        if get_raw:
            return docs

        descriptors = []
        for doc in docs:
            try:
                descriptors.append(doc['doc']['data'])
            except KeyError:
                continue
            except IndexError:
                break
        return descriptors

    def get_shell_descriptors(self, source_name: str, limit: int, cursor: int) -> List[dict]:
        descriptors = []
        if source_name is None:
            rows = self.get_all_docs(limit=limit, skip=cursor)
        else:
            rows = self.get_view_docs(source_name, limit, cursor)

        for row in rows:
            try:
                descriptors.append(
                    row['doc']['data']
                )
            except KeyError:
                continue
            except IndexError:
                break

        return descriptors

    def delete_shell_descriptor(self, aas_identifier: str):
        self.delete_doc(doc_id=aas_identifier)
