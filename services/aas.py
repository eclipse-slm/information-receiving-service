from typing import Set, List, Dict

from aas_python_http_client import ApiClient, AssetAdministrationShellDescriptor
from basyx.aas.model import Submodel

from dotenv import load_dotenv
import requests

import services.edc.edr as edr
import services.edc.contract_negotiations as negotiations
import services.edc.contract_agreements as agreements
import services.edc.catalog as catalog
from tx_edc_connector_client.models.criterion import Criterion
from tx_edc_connector_client.models.query_spec import QuerySpec
from tx_edc_connector_client.models.catalog import Catalog

from aas.couch_db_basyx_client import aas_obj_store
from logger.logger import LOG
from services.aas_local import write_shells_based_on_remote_descriptors, write_submodels, write_submodel_references, \
    delete_shells_base_on_remote_descriptors, delete_submodels, delete_submodel_references, cache_submodels
from services.aas_utils import SubmodelEdcAsset, get_edc_asset_ids_of_submodels_from_shell_descriptor, decode_id

load_dotenv()


DEFAULT_DATASET_EDC_TYPE = "data.core.digitalTwinRegistry"


class ApiResponse:
    def __init__(self, data):
        self.data = data


def crawl_remote_aas_resources():
    LOG.info("Start crawling of remote aas resources...")

    # Get Shell Descriptors and Create Shells in local repository:
    shell_descriptors = get_remote_shell_descriptors()
    write_shells_based_on_remote_descriptors(shell_descriptors)
    delete_shells_base_on_remote_descriptors(shell_descriptors)

    # Get Submodels and Create them in local repository:
    remote_submodels = []
    for shell_descriptor in shell_descriptors:
        # Get Submodel | EDC Mapping:
        submodel_edc_assets = get_edc_asset_ids_of_submodels_from_shell_descriptor(shell_descriptor)

        submodels = get_remote_submodels_by_edc_assets(submodel_edc_assets)
        write_submodels(submodels)
        cache_submodels(shell_descriptor.id, submodels)
        remote_submodels.extend(submodels)

    # Delete local Submodels that are not in remote repositories:
    delete_submodels(remote_submodels)

    # Link Shells to submodels with submodel references in shell repository:
    write_submodel_references(shell_descriptors)
    # Delete residual submodel references in local shell:
    delete_submodel_references(shell_descriptors)


def get_edc_asset_ids_of_shell_registries(negotiated: bool = False) -> Set[str]:
    asset_id_list = set()
    contract_negotiations = negotiations.get_contract_negotiations()

    for cn in contract_negotiations:
        counter_part_address = cn.counter_party_address
        contract_agreement_id = cn.contract_agreement_id
        if contract_agreement_id is None:
            continue
        contract_agreement = agreements.get_contract_agreement_by_id(contract_agreement_id)
        asset_id = contract_agreement.asset_id
        dataset = catalog.get_dataset(asset_id, counter_part_address)

        if dataset is not None and dataset.type == DEFAULT_DATASET_EDC_TYPE:
            asset_id_list.add(asset_id)

    if negotiated:
        return get_negotiated_asset_ids(asset_id_list)
    else:
        return asset_id_list


def get_edc_asset_ids_of_submodels(
        shell_registry_asset_id: str,
        shell_descriptor_id_enc: str = "",
        negotiated: bool = False
) -> Set[str]:
    if shell_descriptor_id_enc == "":
        search_result = get_remote_shell_descriptors_of_shell_registry_by_asset_id(shell_registry_asset_id)
    else:
        shell_descriptor_id = decode_id(shell_descriptor_id_enc)
        search_result = get_remote_shell_descriptor_by_asset_id(shell_registry_asset_id, shell_descriptor_id)

    if len(search_result) == 0:
        return set()

    asset_ids = set()
    for descriptor in search_result:
        submodel_edc_asset_list = get_edc_asset_ids_of_submodels_from_shell_descriptor(descriptor)
        asset_ids.update([asset.edc_asset_id for asset in submodel_edc_asset_list])

    if negotiated:
        return get_negotiated_asset_ids(asset_ids)
    else:
        return asset_ids


def get_remote_shell_descriptor_by_asset_id(shell_registry_asset_id: str, shell_descriptor_id: str) -> List[AssetAdministrationShellDescriptor]:
    descriptors = get_remote_shell_descriptors_of_shell_registry_by_asset_id(shell_registry_asset_id)
    descriptors_filtered = [descriptor for descriptor in descriptors if descriptor.id == shell_descriptor_id]

    if len(descriptors_filtered) == 0:
        return []
    if len(descriptors_filtered) > 1:
        LOG.warn(f"Multiple descriptors found for id {shell_descriptor_id}")

    return descriptors_filtered


def get_negotiated_asset_ids(asset_ids: List[str]) -> Set[str]:
    negotiated_asset_ids = set()

    for asset_id in asset_ids:
        edr_list = edr.query_negotiated_edr_by_asset_id(asset_id)
        if len(edr_list) > 0:
            negotiated_asset_ids.add(asset_id)

    return negotiated_asset_ids



def get_catalog_of_shell_registries(counter_part_address: str) -> Catalog:
    assets = catalog.get_catalog(
        counter_party_address=counter_part_address,
        query_spec=QuerySpec(
            offset=0,
            limit=100,
            filter_expression=[Criterion(
                operand_left="https://w3id.org/edc/v0.0.1/ns/type",
                operator="=",
                operand_right=DEFAULT_DATASET_EDC_TYPE
            )]
        )
    )
    return assets



def get_cached_submodel(submodel_id: str):
    try:
        return aas_obj_store.get_identifiable(submodel_id)
    except KeyError as e:
        LOG.info(f"Submodel not found in CouchDB: {e}")
        submodel = get_remote_submodel(submodel_id)
        aas_obj_store.add(submodel)
        return aas_obj_store.get_identifiable(submodel_id)


def get_remote_submodel(submodel_id: str):
    submodels = get_remote_submodels()
    submodel = [submodel for submodel in submodels if submodel.id == submodel_id]

    if len(submodel) == 0:
        LOG.warn(f"No submodel found for id {submodel_id}")
        return None
    if len(submodel) > 1:
        LOG.warn(f"Multiple submodels found for id {submodel_id}")

    return submodel[0]


def get_submodels(cached: bool = True):
    shell_descriptors = get_remote_shell_descriptors()
    for shell_descriptor in shell_descriptors:
        submodel_edc_assets = get_edc_asset_ids_of_submodels_from_shell_descriptor(shell_descriptor)

    if cached:
        return get_cached_submodels(submodel_edc_assets)
    else:
        return get_remote_submodels_by_edc_assets(submodel_edc_assets)


def get_cached_submodels(submodel_edc_assets: Set[SubmodelEdcAsset]) -> List[Submodel]:
    submodels = []
    for submodel_edc_asset in submodel_edc_assets:
        try:
            submodels.append(
                aas_obj_store.get_identifiable(submodel_edc_asset.submodel_id)
            )
        except KeyError as e:
            LOG.info(f"Submodel not found in CouchDB: {e}")
            submodel = get_remote_submodel_by_edc_asset(submodel_edc_asset)
            if submodel is not None:
                aas_obj_store.add(
                    submodel
                )
                submodels.append(
                    aas_obj_store.get_identifiable(submodel_edc_asset.submodel_id)
                )
    return submodels

def get_remote_submodels():
    shell_descriptors = get_remote_shell_descriptors()
    for shell_descriptor in shell_descriptors:
        submodel_edc_assets = get_edc_asset_ids_of_submodels_from_shell_descriptor(shell_descriptor)

    return get_remote_submodels_by_edc_assets(submodel_edc_assets)


def get_remote_submodels_by_edc_assets(submodels_edc_assets: Set[SubmodelEdcAsset]):
    submodels = []

    for submodel_edc_asset in submodels_edc_assets:
        submodel = get_remote_submodel_by_edc_asset(submodel_edc_asset)
        if not submodel is None:
            submodels.append(submodel)

    return submodels


def get_remote_submodel_by_edc_asset(submodel_edc_asset: SubmodelEdcAsset) -> Submodel:
    asset_id = submodel_edc_asset.edc_asset_id

    edr_list = edr.query_negotiated_edr_by_asset_id(asset_id)
    if len(edr_list) == 0:
        LOG.info(f"No EDRs found for asset {asset_id}")
        return None
    transfer_process_id = edr_list[0].transfer_process_id
    data_address = edr.get_data_address_by_transfer_process_id(transfer_process_id)

    return _get_remote_submodel(
        data_address.endpoint,
        data_address.auth_code,
        submodel_edc_asset.submodel_id_enc
    )


def get_remote_shell_descriptors(strip_not_negotiated_sm_descriptors: bool = True) -> List[AssetAdministrationShellDescriptor]:
    asset_id_list = get_edc_asset_ids_of_shell_registries()
    shell_descriptors = []

    for asset_id in asset_id_list:
        edr_list = edr.query_negotiated_edr_by_asset_id(asset_id)
        if len(edr_list) == 0:
            LOG.info(f"No EDRs found for asset {asset_id}")
            continue
        transfer_process_id = edr_list[0].transfer_process_id
        data_address = edr.get_data_address_by_transfer_process_id(transfer_process_id)
        registry_base_url = data_address.endpoint
        auth_code = data_address.auth_code

        descriptors = _get_remote_descriptors(registry_base_url, auth_code)
        shell_descriptors.extend(descriptors)

    # Removes all submodel descriptors if the submodel is not accessible via Connector:
    if strip_not_negotiated_sm_descriptors:
        for shell_descriptor in shell_descriptors:
            sm_asset_ids = get_edc_asset_ids_of_submodels_from_shell_descriptor(shell_descriptor)
            for sm_edc_asset in sm_asset_ids:
                if len(edr.query_negotiated_edr_by_asset_id(sm_edc_asset.edc_asset_id)) == 0:
                    shell_descriptor.submodel_descriptors = [sm for sm in shell_descriptor.submodel_descriptors if sm.id != sm_edc_asset.submodel_id]

    return shell_descriptors


def get_remote_shell_descriptor(shell_descriptor_id: str) -> AssetAdministrationShellDescriptor:
    descriptors = get_remote_shell_descriptors()
    descriptor = [descriptor for descriptor in descriptors if descriptor.id == shell_descriptor_id]

    if len(descriptor) == 0:
        LOG.warn(f"No descriptor found for id {shell_descriptor_id}")
        return None
    if len(descriptor) > 1:
        LOG.warn(f"Multiple descriptors found for id {shell_descriptor_id}")

    return descriptor[0]


def get_remote_shell_descriptors_of_shell_registry_by_asset_id(asset_id: str) -> List[AssetAdministrationShellDescriptor]:
    edr_list = edr.query_negotiated_edr_by_asset_id(asset_id)
    if len(edr_list) == 0:
        LOG.info(f"No EDRs found for asset {asset_id}")
        return []
    transfer_process_id = edr_list[0].transfer_process_id
    data_address = edr.get_data_address_by_transfer_process_id(transfer_process_id)
    registry_base_url = data_address.endpoint
    auth_code = data_address.auth_code

    return _get_remote_descriptors(registry_base_url, auth_code)


def _get_remote_descriptors(base_url: str, auth_code: str):
    url = f"{base_url}/shell-descriptors"

    response = requests.request("GET", url, headers=create_auth_header(auth_code))
    descriptor_result = ApiClient().deserialize(
        response=ApiResponse(response.content),
        response_type='GetAssetAdministrationShellDescriptorsResult'
    )

    return descriptor_result.result


def _get_remote_submodel(base_url: str, auth_code: str, submodel_id_enc: str) -> Submodel:
    # for bundles:
    # url = f"{base_url}/submodels/{submodel_id_enc}"
    # for single submodels:
    url = f"{base_url}"

    response = requests.request("GET", url, headers=create_auth_header(auth_code))
    if response.status_code != 200:
        return None

    submodel = ApiClient().deserialize(
        response=ApiResponse(response.content),
        response_type='Submodel'
    )
    return submodel


def create_auth_header(auth_code: str) -> Dict:
    return {
        'Authorization': auth_code
    }