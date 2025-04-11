import json
import logging
import urllib
from typing import Iterator, Iterable

from aas_python_http_client import ApiClient, AssetAdministrationShellDescriptor
from basyx.aas.adapter.json import AASToJsonEncoder
from basyx.aas.backend import couchdb
from basyx.aas.backend.couchdb import CouchDBObjectStore, CouchDBBackend
from basyx.aas.model import SpecificAssetId, ExternalReference

logger = logging.getLogger(__name__)


class CrawlerCouchDBObjectStore(CouchDBObjectStore):
    def __init__(self, url: str, db_name: str):
        super().__init__(url, db_name)
        self._db_name = db_name
        self._url = url
        self._db = None
        self._api_client: ApiClient = ApiClient()

    def _specific_asset_id_serializer(self, obj):
        if isinstance(obj, SpecificAssetId) or isinstance(obj, ExternalReference):
            return json.loads(json.dumps(obj, cls=AASToJsonEncoder))
        raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

    def add_descriptor(self, x):
        logger.debug("Adding descriptor %s to CouchDB database ...", repr(x))
        # Serialize data
        data = json.dumps({'data': x.to_dict()}, default=self._specific_asset_id_serializer)

        # Create and issue HTTP request (raises HTTPError on status != 200)

        try:
            response = CouchDBBackend.do_request(
                "{}/{}/{}".format(self.url, self.database_name, self._transform_id(x.id)),
                'PUT',
                {'Content-type': 'application/json'},
                data.encode('utf-8'))
            couchdb.set_couchdb_revision(
                "{}/{}/{}".format(self.url, self.database_name, self._transform_id(x.id)),
                response["rev"]
            )
        except couchdb.CouchDBServerError as e:
            if e.code == 409:
                raise KeyError(f"Identifiable with id {x.id} already exists in CouchDB database") from e
            raise
        with self._object_cache_lock:
            self._object_cache[x.id] = x
        self.generate_source(x)  # Set the source of the object

    def get_descriptor(self, identifier: str):
        try:
            return self.get_descriptor_by_couchdb_id(self._transform_id(identifier, False))
        except KeyError as e:
            raise KeyError("No Identifiable with id {} found in CouchDB database".format(identifier)) from e

    def get_descriptor_by_couchdb_id(self, couchdb_id: str):
        try:
            data = CouchDBBackend.do_request(
                "{}/{}/{}".format(self.url, self.database_name, urllib.parse.quote(couchdb_id, safe='')))
        except couchdb.CouchDBServerError as e:
            if e.code == 404:
                raise KeyError("No Identifiable with couchdb-id {} found in CouchDB database".format(couchdb_id)) from e
            raise

        # Add CouchDB meta data (for later commits) to object
        obj = AssetAdministrationShellDescriptor(**data['data'])
        # self.generate_source(obj)  # Generate the source parameter of this object
        couchdb.set_couchdb_revision("{}/{}/{}".format(self.url, self.database_name, urllib.parse.quote(couchdb_id, safe='')),
                                     data["_rev"])

        # If we still have a local replication of that object (since it is referenced from anywhere else), update that
        # replication and return it.
        with self._object_cache_lock:
            if obj.id in self._object_cache:
                old_obj = self._object_cache[obj.id]
                # If the source does not match the correct source for this CouchDB backend, the object seems to belong
                # to another backend now, so we return a fresh copy
                # if old_obj.source == obj.source:
                #     old_obj.update_from(obj)
                #     return old_obj

        self._object_cache[obj.id] = obj
        return obj


    def __iter__(self) -> Iterator[AssetAdministrationShellDescriptor]:
        """
        Iterate all :class:`~aas.model.base.Identifiable` objects in the CouchDB database.

        This method returns a lazy iterator, containing only a list of all identifiers in the database and retrieving
        the identifiable objects on the fly.

        :raises CouchDBError: If error occur during fetching the list of objects from the CouchDB server (see
                              `_do_request()` for details)
        """
        # Iterator class storing the list of ids and fetching Identifiable objects on the fly
        class CouchDBIdentifiableIterator(Iterator[AssetAdministrationShellDescriptor]):
            def __init__(self, store: CrawlerCouchDBObjectStore, ids: Iterable[str]):
                self._iter = iter(ids)
                self._store = store

            def __next__(self):
                next_id = next(self._iter)
                return self._store.get_descriptor_by_couchdb_id(next_id)

        # Fetch a list of all ids and construct Iterator object
        logger.debug("Creating iterator over objects in database ...")
        data = CouchDBBackend.do_request("{}/{}/_all_docs".format(self.url, self.database_name))
        return CouchDBIdentifiableIterator(self, (row['id'] for row in data['rows']))
