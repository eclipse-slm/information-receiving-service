import os
from typing import List

from dotenv import load_dotenv

from tx_edc_connector_client.api_client import ApiClient
from tx_edc_connector_client.configuration import Configuration
from tx_edc_connector_client.exceptions import ApiException
from tx_edc_connector_client.api.contract_negotiation_api import ContractNegotiationApi
from tx_edc_connector_client.models.contract_negotiation import ContractNegotiation
from logger.logger import LOG

load_dotenv()

configuration = Configuration(
    host = os.getenv("CONNECTOR_BASE_URL")+"/management"
)
api_client = ApiClient(configuration, header_name='X-Api-Key', header_value=os.getenv("CONNECTOR_XAPI_KEY"))
contract_negotiation_api = ContractNegotiationApi(api_client)

filter_name_to_property_name = {
    "state": "edcstate",
    "counter_party_id": "edccounter_party_id",
    "counter_party_address": "edccounter_party_address",
    "contract_agreement_id": "edccontract_agreement_id"
}

def get_contract_negotiations() -> List[ContractNegotiation]:
    try:
        return contract_negotiation_api.query_negotiations()
    except ApiException as e:
        LOG.info("Exception when calling ContractNegotiationApi->get_contract_negotiations: %s\n" % e)

def get_contract_negotiations_filtered(
        filters: dict
) -> List[ContractNegotiation]:
    try:
        contract_negotiations = contract_negotiation_api.query_negotiations()

        for filter_name, property_value in filters.items():
            if filter_name not in filter_name_to_property_name:
                continue

            if property_value:
                contract_negotiations = [
                    cn for cn in contract_negotiations if getattr(cn, filter_name_to_property_name[filter_name]) == property_value
                ]

        return contract_negotiations
    except ApiException as e:
        LOG.info("Exception when calling ContractNegotiationApi->get_contract_negotiations: %s\n" % e)
