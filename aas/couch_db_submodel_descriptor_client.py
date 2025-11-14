import json
from typing import List

from aas_python_http_client import SubmodelDescriptor
from basyx.aas.adapter.json import AASToJsonEncoder
from basyx.aas.model import ExternalReference

from aas.couch_db_client import CouchDBClient


def _serializer(obj):
    if isinstance(obj, ExternalReference):
        return json.loads(json.dumps(obj, cls=AASToJsonEncoder))
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


class CouchDBSubmodelDescriptorClient(CouchDBClient):
    """
    A client for interacting with a CouchDB database to store and retrieve Submodel (SM) descriptors.
    """

    def __init__(self, client_name: str):
        super().__init__(database_name="submodel_descriptors", client_name=client_name)
        self._create_database()


    def get_submodel_descriptor(self, identifier: str) -> dict:
        """
        Get a submodel descriptor by its identifier.

        Args:
            identifier (str): The identifier of the submodel descriptor.

        Returns:
            SubmodelDescriptor: The submodel descriptor.
        """
        doc = self.get_doc(doc_id=identifier)
        if doc is None:
            return None
        # return SubmodelDescriptor(**doc['data'])
        return doc['data']


    def get_all_submodel_descriptors(self, get_raw: bool = False) -> List[dict]:
        docs = self.get_all_docs()

        for doc in docs:
            if "_design" in doc['id']:
                docs.remove(doc)
                break

        if get_raw:
            return docs

        submodel_descriptors = []
        for doc in docs:
            try:
                submodel_descriptors.append(
                    doc['doc']['data']
                    # SubmodelDescriptor(**doc['doc']['data'])
                )
            except KeyError:
                continue
        return submodel_descriptors

    def get_submodel_descriptors(self, source_name: str, limit: int, cursor: int) -> List[dict]:
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


    def save_submodel_descriptors(self, source_name: str, descriptors: list[SubmodelDescriptor]):
        self.save_entities(source_name, descriptors)

    def delete_submodel_descriptor(self, sm_identifier: str):
        self.delete_doc(doc_id=sm_identifier)