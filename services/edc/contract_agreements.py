import os
from typing import List

from dotenv import load_dotenv

from tx_edc_connector_client import ApiClient
from tx_edc_connector_client.configuration import Configuration
from tx_edc_connector_client.exceptions import ApiException
from tx_edc_connector_client.api.contract_agreement_api import ContractAgreementApi
from tx_edc_connector_client.models.contract_agreement import ContractAgreement
from logger.logger import LOG
from services.edc.contract_negotiations import filter_name_to_property_name
import services.edc.edr as edr
from services.edc.edr import EdrState

load_dotenv()

configuration = Configuration(
    host = os.getenv("CONNECTOR_BASE_URL")+"/management"
)
api_client = ApiClient(configuration, header_name='X-Api-Key', header_value=os.getenv("CONNECTOR_XAPI_KEY"))
contract_agreement_api = ContractAgreementApi(api_client)

filter_name_to_property_name = {
    "asset_id": "edcasset_id",
    "consumer_id": "edcconsumer_id",
    "provider_id": "edcprovider_id"
}

def get_contract_agreements() -> List[ContractAgreement]:
    try:
        return contract_agreement_api.query_all_agreements()
    except ApiException as e:
        LOG.info("Exception when query all contract agreements: %s\n" % e)

def get_contract_agreements_filtered(
        filters: dict,
        edr_state: EdrState = None
) -> List[ContractAgreement]:
    try:
        contract_agreements = contract_agreement_api.query_all_agreements()

        for filter_name, property_value in filters.items():
            if property_value:
                contract_agreements = [
                    ca for ca in contract_agreements if getattr(ca, filter_name_to_property_name[filter_name]) == property_value
                ]

        if edr_state:
            contract_agreements = [
                ca for ca in contract_agreements if edr.query_edr_by_agreement_id(ca.id, edr_state)
            ]

        return contract_agreements
    except ApiException as e:
        LOG.info("Exception when query all contract agreements: %s\n" % e)

def get_contract_agreement_by_id(contract_agreement_id: str) -> ContractAgreement:
    try:
        return contract_agreement_api.get_agreement_by_id(contract_agreement_id)
    except ApiException as e:
        LOG.info("Exception when query all contract agreements: %s\n" % e)

