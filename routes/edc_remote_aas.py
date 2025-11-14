import json
from typing import Annotated

from basyx.aas.adapter.json import AASToJsonEncoder
from fastapi import APIRouter, Query

from tx_edc_connector_client.models.catalog import Catalog

from services import aas
from services.aas_utils import decode_id

router = APIRouter(prefix="/api/aas", tags=["remote_aas"])

@router.get(path="/registries/counter_part", status_code=200, description="Get Catalog of Shell Registries of counter part")
def get_edc_assets_of_shell_registries(
        counter_part_address: Annotated[str, Query(example="http://dataprovider-controlplane.tx.test/api/v1/dsp")]
) -> Catalog | None:
    return aas.get_catalog_of_shell_registries(counter_part_address)

@router.get(path="/registries/negotiated", status_code=200, description="Get available Shell Registries based on contract negotiations")
def get_available_shell_registries():
    return aas.get_edc_asset_ids_of_shell_registries(True)

@router.get(path="/registries/negotiated/{edc_asset_id}/submodel_assets", status_code=200, description="Get Shell Descriptors provided by remote shell registry")
def get_available_submodel_assets_of_shell_registry(edc_asset_id: str, shell_descriptor_id_enc: str = "", negotiated: bool = False):
    return aas.get_edc_asset_ids_of_submodels(edc_asset_id, shell_descriptor_id_enc, negotiated)

@router.get(path="/registries/negotiated/{edc_asset_id}/shell_descriptors", status_code=200, description="Get Shell Descriptors provided by remote shell registry")
def get_shell_descriptors(edc_asset_id: str):
    shell_descriptors = aas.get_remote_shell_descriptors_of_shell_registry_by_asset_id(edc_asset_id)
    if shell_descriptors is None:
        return []
    return [shell_descriptor.to_dict() for shell_descriptor in shell_descriptors]

@router.get(path="/registries/negotiated/{edc_asset_id}/shell_descriptors/{shell_descriptor_id_enc}", status_code=200, description="Get Shell Descriptor provided by remote shell registry")
def get_shell_descriptor(edc_asset_id: str, shell_descriptor_id_enc: str):
    shell_descriptor = aas.get_remote_shell_descriptor_by_asset_id(edc_asset_id, decode_id(shell_descriptor_id_enc))
    return shell_descriptor[0].to_dict()

@router.get(response_model=None, path="/registries/negotiated/{edc_asset_id}/shell_descriptors/{shell_descriptor_id_enc}/submodels", status_code=200, description="Get all submodels of negotiated edc submodel bundles")
def get_submodels_of_shell_registry_asset(edc_asset_id: str, shell_descriptor_id_enc: str):
    shell_descriptor_id = decode_id(shell_descriptor_id_enc)
    search_result = aas.get_remote_shell_descriptor_by_asset_id(edc_asset_id, shell_descriptor_id)
    descriptor = search_result[0]
    submodel_edc_assets = aas.get_edc_asset_ids_of_submodels_from_shell_descriptor(descriptor)
    submodels = aas.get_remote_submodels_by_edc_assets(submodel_edc_assets)
    return json.loads(json.dumps(submodels, cls=AASToJsonEncoder))

@router.get(path="/registries/negotiated/{edc_asset_id}/shell_descriptors/{shell_descriptor_id_enc}/submodels/{edc_asset_id_submodel}", status_code=200, description="Get submodels of one negotiated edc submodel bundle")
def get_submodel_bundle_of_shell_registry_asset(edc_asset_id: str, shell_descriptor_id_enc: str, edc_asset_id_submodel: str):
    shell_descriptor_id = decode_id(shell_descriptor_id_enc)
    search_result = aas.get_remote_shell_descriptor_by_asset_id(edc_asset_id, shell_descriptor_id)
    descriptor = search_result[0]
    submodel_edc_assets = aas.get_edc_asset_ids_of_submodels_from_shell_descriptor(descriptor)
    submodel_edc_asset = [asset for asset in submodel_edc_assets if asset.edc_asset_id == edc_asset_id_submodel]

    submodels = aas.get_remote_submodels_by_edc_assets(submodel_edc_asset)

    return json.loads(json.dumps(submodels, cls=AASToJsonEncoder))