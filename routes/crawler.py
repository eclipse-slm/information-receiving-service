from fastapi import APIRouter
from tx_edc_connector_client import QuerySpec, Criterion, Policy

from services import aas, crawler
import services.edc.contract_negotiations as negotiations
import services.edc.contract_agreements as agreements
import services.edc.edr as edr
from  services import negotiated_datasets
from services.edc.catalog import get_catalog
from services.edc.counter_parts import get_counter_part_addresses, add_counter_part_address, remove_counter_part_address

# router = APIRouter(prefix="/api/crawler", tags=["crawler"])
router = APIRouter(tags=["crawler"])

@router.post(path="/", status_code=200, description="Start Crawling")
def start_crawling_manually():
    aas.crawl_remote_aas_resources()


@router.post(path="/polling", status_code=200, description="Start/Stop automatic Crawling")
def start_stop_automatic_crawling():
    return crawler.start_stop_automatic_crawling()


@router.post(path="/asset/negotiate", status_code=201, description="Run negotiation for asset")
def do_negotiation_for_asset(counter_part_address: str, asset_id: str):
    query_spec = QuerySpec(
        offset=0,
        limit=100,
        filter_expression=[
            Criterion(
                operand_left="https://w3id.org/edc/v0.0.1/ns/id",
                operand_right=asset_id,
                operator="="
            )
        ]
    )

    catalog = get_catalog(counter_party_address=counter_part_address, query_spec=query_spec)
    participant_id = catalog.participant_id
    asset_id = catalog.dcatdataset['@id']
    offer_id = catalog.dcatdataset['odrl:hasPolicy']['@id']
    policy = Policy(
        odrlobligation=catalog.dcatdataset['odrl:hasPolicy']['odrl:obligation'],
        odrlpermission=catalog.dcatdataset['odrl:hasPolicy']['odrl:permission'],
        odrlprohibition=catalog.dcatdataset['odrl:hasPolicy']['odrl:prohibition'],
        odrltarget=catalog.dcatdataset['odrl:hasPolicy']['odrl:target']
    )


    edr.request_edr(
        counter_party_address=counter_part_address,
        provider_id=participant_id,
        asset_id=asset_id,
        offer_id=offer_id,
        policy=policy
    )

    return None


@router.get(path="/contract_negotiations", status_code=200, description="Get all contract negotiations")
def get_contract_negotiations(
        state: str = None,
        counter_party_id: str = None,
        counter_party_address: str = None,
        contract_agreement_id: str = None
):
    return negotiations.get_contract_negotiations_filtered(
        {
            "state": state,
            "counter_party_id": counter_party_id,
            "counter_party_address": counter_party_address,
            "contract_agreement_id": contract_agreement_id
        }
    )


@router.get(path="/contract_agreements", status_code=200, description="Get all contract agreements")
def get_contract_agreements(
        asset_id: str = None,
        consumer_id: str = None,
        provider_id: str = None,
        edr_state: edr.EdrState = None
):
    return agreements.get_contract_agreements_filtered(
        {
            "asset_id": asset_id,
            "consumer_id": consumer_id,
            "provider_id": provider_id
        },
        edr_state
    )

@router.get(path="/edr/by-asset/{asset_id}", status_code=200, description="Get all EDRs by asset id")
def get_edrs_by_asset_id(asset_id: str, state: edr.EdrState = None):
    return edr.query_edr_by_asset_id(asset_id, state)

@router.get(path="/edr/by-agreement/{agreement_id}", status_code=200, description="Get all EDRs by agreement id")
def get_edrs_by_agreement_id(agreement_id: str, state: edr.EdrState = None):
    return edr.query_edr_by_agreement_id(agreement_id, state)

@router.get(path="/negotiated_datasets", status_code=200, description="Get all negotiated datasets")
def get_negotiated_datasets(asset_id: str = None, type: str = None):
    return negotiated_datasets.get_negotiated_datasets_filtered(
        {
            "id": asset_id,
            "type": type
        }
    )

@router.get(path="/counter_part_addresses", status_code=200, description="Get all counter part addresses")
def get_counter_party_addresses():
    return get_counter_part_addresses()

@router.post(path="/counter_part_addresses", status_code=201, description="Add counter part address")
def get_counter_party_addresses(counter_part_address: str):
    return add_counter_part_address(counter_part_address)

@router.delete(path="/counter_part_addresses", status_code=200, description="Remove counter part address")
def get_counter_party_addresses(counter_part_address: str):
    return remove_counter_part_address(counter_part_address)