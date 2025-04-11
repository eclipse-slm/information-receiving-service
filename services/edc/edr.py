import os
from enum import Enum
from typing import List

from dotenv import load_dotenv
from tx_edc_connector_client import NegotiateEdrRequest,  NegotiateEdrRequestContext, \
    ContractOfferDescription

from logger.logger import LOG

from tx_edc_connector_client.api_client import ApiClient
from tx_edc_connector_client.configuration import Configuration
from tx_edc_connector_client.exceptions import ApiException
from tx_edc_connector_client.api.control_plane_edr_api_api import ControlPlaneEDRApiApi
from tx_edc_connector_client.models.endpoint_data_reference_entry import EndpointDataReferenceEntry
from tx_edc_connector_client.models.data_address import DataAddress
from tx_edc_connector_client import Policy

load_dotenv()

configuration = Configuration(
    host = os.getenv("CONNECTOR_BASE_URL")+"/management"
)
api_client = ApiClient(configuration, header_name='X-Api-Key', header_value=os.getenv("CONNECTOR_XAPI_KEY"))
edr_api = ControlPlaneEDRApiApi(api_client)

class EdrState(str, Enum):
    NEGOTIATED = "NEGOTIATED"
    REFRESHING = "REFRESHING"


def query_edr_by_agreement_id(agreement_id: str, state: EdrState = None)-> List[EndpointDataReferenceEntry]:
    try:
        edrs = edr_api.query_edrs(agreement_id=agreement_id)
        if state:
            return [edr for edr in edrs if edr.txedr_state == state]

        return edrs
    except ApiException as e:
        LOG.info("Exception when query all edr by agreement_id: %s\n" % e)


def query_edr_by_asset_id(asset_id: str, state: EdrState = None)-> List[EndpointDataReferenceEntry]:
    try:
        edrs = edr_api.query_edrs(asset_id=asset_id)
        if state:
            return [edr for edr in edrs if edr.txedr_state == state]

        return edrs
    except ApiException as e:
        LOG.info("Exception when query all edr by asset_id: %s\n" % e)

def query_negotiated_edr_by_asset_id(asset_id: str)-> List[EndpointDataReferenceEntry]:
    edrs = query_edr_by_asset_id(asset_id=asset_id, state=EdrState.NEGOTIATED.name)

    return edrs

def get_data_address_by_transfer_process_id(transfer_process_id: str) -> DataAddress:
    try:
        return edr_api.get_edr(transfer_process_id)
    except ApiException as e:
        LOG.info("Exception when getting edr by transfer_process_id: %s\n" % e)

def request_edr(counter_party_address: str, provider_id: str, asset_id: str, offer_id:str, policy: Policy):
    offer = ContractOfferDescription(
        asset_id=asset_id,
        offer_id=offer_id,
        policy=policy
    )
    request = NegotiateEdrRequest(
        context=NegotiateEdrRequestContext(),
        counter_party_address=counter_party_address,
        counter_party_id=provider_id,
        provider_id=provider_id,
        offer=offer
    )

    result = edr_api.initiate_edr_negotiation(negotiate_edr_request=request)
    return
