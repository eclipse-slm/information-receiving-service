import os
from dotenv import load_dotenv
from pydantic_core import ValidationError

from tx_edc_connector_client.api_client import ApiClient
from tx_edc_connector_client.configuration import Configuration
from tx_edc_connector_client.exceptions import  ServiceException
from tx_edc_connector_client.api.catalog_api import CatalogApi
from tx_edc_connector_client.models.catalog import Catalog
from tx_edc_connector_client.models.catalog_request import CatalogRequest
from tx_edc_connector_client.models.dataset import Dataset
from tx_edc_connector_client.models.dataset_request import DatasetRequest
from tx_edc_connector_client.models.dataset_request_context import DatasetRequestContext
from tx_edc_connector_client.models.query_spec import QuerySpec

from logger.logger import LOG

load_dotenv()

DEFAULT_CATALOG_FILTER = QuerySpec(
    offset=0,
    limit=100,
    filter_expression=[]
)

configuration = Configuration(
    host = os.getenv("CONNECTOR_BASE_URL")+"/management"
)
api_client = ApiClient(configuration, header_name='X-Api-Key', header_value=os.getenv("CONNECTOR_XAPI_KEY"))
catalog_api = CatalogApi(api_client)


def get_catalog(counter_party_address: str, protocol: str = "dataspace-protocol-http", query_spec: QuerySpec = DEFAULT_CATALOG_FILTER) -> Catalog:
    catalog_request = CatalogRequest(
        context={
            "edc": "https://w3id.org/edc/v0.0.1/ns/"
        },
        counter_party_address=counter_party_address,
        protocol=protocol,
        query_spec=query_spec
    )
    try:
        return catalog_api.request_catalog(catalog_request=catalog_request)
    except ServiceException as e:
        if "Name does not resolve" in e.body:
            LOG.warn(f"Connector not reachable because name '{counter_party_address}' does not resolve")
        else:
            print(f"ServiceException when calling CatalogApi->request_catalog: {e}")
        return None


def get_dataset(asset_id: str, counter_party_address: str, protocol: str = "dataspace-protocol-http", ) -> Dataset:
    request = DatasetRequest(
        context=DatasetRequestContext(),
        type="DatasetRequest",
        id=asset_id,
        counter_party_address=counter_party_address,
        protocol=protocol
    )
    try:
        return catalog_api.get_dataset(request)
    except ValidationError as e:
        print(f"ValidationError when calling CatalogApi->get_dataset: {e}")
        return None
