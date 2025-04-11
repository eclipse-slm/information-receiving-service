import os
from typing import List

from dotenv import load_dotenv

from tx_edc_connector_client.api_client import ApiClient
from tx_edc_connector_client.configuration import Configuration
from tx_edc_connector_client.exceptions import ApiException
from tx_edc_connector_client.api.asset_api import AssetApi
from tx_edc_connector_client.models.asset_output import AssetOutput
from logger.logger import LOG

load_dotenv()

configuration = Configuration(
    host = os.getenv("CONNECTOR_BASE_URL")+"/management"
)
api_client = ApiClient(configuration, header_name='X-Api-Key', header_value=os.getenv("CONNECTOR_XAPI_KEY"))
asset_api = AssetApi(api_client)

def get_assets() -> List[AssetOutput]:
    try:
        return asset_api.request_assets1()
    except ApiException as e:
        LOG.info("Exception when calling AssetApi->get_assets: %s\n" % e)