# Get config
import base64
import os
import urllib

import pycouchdb
from basyx.aas.backend import couchdb
from dotenv import load_dotenv
from pycouchdb.exceptions import Conflict

from logger.logger import LOG

load_dotenv()

# AAS Object Storage:
couchdb_url = 'http://'+os.getenv('COUCHDB_HOST')+':'+os.getenv('COUCHDB_PORT')
couchdb_database = os.getenv('COUCHDB_DB_NAME')
couchdb_user = os.getenv('COUCHDB_USERNAME')
couchdb_password = os.getenv('COUCHDB_PASSWORD')
couchdb_url_with_creds = 'http://'+couchdb_user+':'+couchdb_password+'@'+os.getenv('COUCHDB_HOST')+':'+os.getenv('COUCHDB_PORT')
couchdb_default_headers = {
    'Accept': 'application/json',
    'Authorization': 'Basic {0}'.format(base64.b64encode(
        bytes('{0}:{1}'.format(couchdb_user, couchdb_password), 'utf-8')
    ).decode('ascii'))
}
server = pycouchdb.Server(base_url=couchdb_url_with_creds,authmethod="basic")
try:
    server.create(couchdb_database)
except Conflict:
    LOG.info("CouchDB database {} already existed.".format(couchdb_database))
db = server.database(couchdb_database)

couchdb.register_credentials(couchdb_url, couchdb_user, couchdb_password)

request = urllib.request.Request(
    "{}/{}".format(couchdb_url, couchdb_database),
    headers=couchdb_default_headers,
    method='PUT'
)
try:
    urllib.request.urlopen(request)
except urllib.error.HTTPError as e:
    if e.code == 412:
        LOG.info("CouchDB database {} already existed.".format(couchdb_database))
    else:
        raise

aas_obj_store = couchdb.CouchDBObjectStore(couchdb_url, couchdb_database)

