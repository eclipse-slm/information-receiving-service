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


    def get_all_submodel_descriptors(self) -> List[dict]:
        """
        Get all submodel descriptors from the CouchDB database.

        Returns:
            List[SubmodelDescriptor]: A list of submodel descriptors.
        """
        docs = self.get_all_docs()
        submodel_descriptors = []
        for doc in docs:
            submodel_descriptors.append(
                doc['doc']['data']
                # SubmodelDescriptor(**doc['doc']['data'])
            )
        return submodel_descriptors


    def save_submodel_descriptors(self, descriptors: list[SubmodelDescriptor]):
        self.save_entities(descriptors)

    def delete_submodel_descriptor(self, sm_identifier: str):
        self.delete_doc(doc_id=sm_identifier)