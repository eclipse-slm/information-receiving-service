import base64
import json
import os
from typing import Set, List

from aas_python_http_client import AssetAdministrationShellDescriptor, AssetAdministrationShell, ModelType, \
    AssetInformation, AssetKind, Reference, ReferenceTypes, Key, Endpoint, ProtocolInformation, SubmodelDescriptor, \
    ApiClient
from basyx.aas.adapter.json import json_deserialization

api_client = ApiClient()
DEFAULT_SUBPROTOCOL_NAME = "DSP"

class SubmodelEdcAsset:
    def __init__(self, edc_asset_id, submodel_id, dsp_endpoint, dataplane_endpoint):
        self.edc_asset_id = edc_asset_id
        self.submodel_id = submodel_id
        self.dsp_endpoint = dsp_endpoint
        self.dataplane_endpoint = dataplane_endpoint

    @property
    def submodel_id_enc(self) -> str:
        return encode_id(self.submodel_id)

    # @property
    # def submodel_id_enc(self) -> str:
    #     return self.dataplane_endpoint.split("/")[-1]
    #
    # @property
    # def submodel_id(self) -> str:
    #     return base64.b64decode(
    #         self.submodel_id_enc
    #     ).decode("utf-8")

    def __eq__(self, other):
        if isinstance(other, SubmodelEdcAsset):
            return (
                    self.edc_asset_id == other.edc_asset_id and
                    self.dataplane_endpoint == other.dataplane_endpoint
            )
        return NotImplemented

    def __hash__(self):
        return hash(self.edc_asset_id+self.dataplane_endpoint)

def get_base_url():
    return f"{os.getenv('SERVICE_BASE_URL')}:{os.getenv('SERVICE_PORT')}"

def get_base_url_shell_repo():
    return f"{get_base_url()}/api/shell_repo"

def get_base_url_submodel_repo():
    return f"{get_base_url()}/api/submodel_repo"

def fix_base64_padding(b64_string):
    """Fix Base64 padding if required."""
    # Calculate the number of padding characters needed
    padding_needed = len(b64_string) % 4
    if padding_needed:
        # Add the necessary padding
        b64_string += '=' * (4 - padding_needed)
    return b64_string


def encode_id(id: str) -> str:
    return base64.b64encode(id.encode("utf-8")).decode("utf-8")


def decode_id(id_enc: str) -> str:
    return base64.b64decode(
        fix_base64_padding(id_enc)
    ).decode("utf-8")


def get_edc_asset_ids_of_submodels_from_shell_descriptor(descriptor: AssetAdministrationShellDescriptor) -> Set[SubmodelEdcAsset]:
    submodel_s_edc_assets = set()
    submodel_descriptors = descriptor.submodel_descriptors
    if submodel_descriptors is not None:
        for submodel_descriptor in submodel_descriptors:
            for endpoint in submodel_descriptor.endpoints:
                prot_info = endpoint.protocol_information
                if prot_info.subprotocol == DEFAULT_SUBPROTOCOL_NAME:
                    subprot_body_split = prot_info.subprotocol_body.split(";")
                    asset_id = subprot_body_split[0].split("=")[1]
                    dsp_endpoint = subprot_body_split[1].split("=")[1]
                    dataplane_endpoint = prot_info.href

                    submodel_s_edc_assets.add(
                        SubmodelEdcAsset(
                            asset_id,
                            submodel_descriptor.id,
                            dsp_endpoint,
                            dataplane_endpoint
                        )
                    )

    return submodel_s_edc_assets

def convert_shell_descriptor_to_shell(descriptor: AssetAdministrationShellDescriptor) -> AssetAdministrationShell:
    global_asset_id = descriptor.global_asset_id if descriptor.global_asset_id is not None else descriptor.id
    model_type = ModelType.ASSETADMINISTRATIONSHELL

    aas = AssetAdministrationShell(
        model_type=model_type,
        asset_information=AssetInformation(
            asset_kind=AssetKind.INSTANCE,
            global_asset_id=global_asset_id,
        ),
        id=descriptor.id,
        id_short=descriptor.id_short
    )

    aas.model_type = model_type

    return aas

def convert_shell_descriptors_to_shells(shell_descriptors: List[AssetAdministrationShellDescriptor]) -> List[AssetAdministrationShell]:
    shells = []
    for descriptor in shell_descriptors:
        aas = convert_shell_descriptor_to_shell(descriptor)
        shells.append(aas)

    return shells

def extract_submodel_references_from_shell_descriptor(shell_descriptor: AssetAdministrationShellDescriptor) -> List[Reference]:
    references = []
    if isinstance(shell_descriptor, AssetAdministrationShellDescriptor):
        if shell_descriptor.submodel_descriptors is not None:
            for submodel_descriptor in shell_descriptor.submodel_descriptors:
                if isinstance(submodel_descriptor, SubmodelDescriptor):
                    submodel_id = submodel_descriptor.id
                else:
                    submodel_id = submodel_descriptor['id']
                references.append(_create_reference_by_submodel_id(submodel_id))
    else:
        for submodel_descriptor in shell_descriptor['submodelDescriptors']:
            submodel_id = submodel_descriptor['id']
            references.append(_create_reference_by_submodel_id(submodel_id))

    return references

def _create_reference_by_submodel_id(submodel_id: str) -> Reference:
    return Reference(
        type=ReferenceTypes.MODELREFERENCE,
        keys=[Key(type=ModelType.SUBMODEL, value=submodel_id)],
    )

def convert_submodel_descriptor_endpoints_to_local_href(submodel_descriptors: List[SubmodelDescriptor]):
    if submodel_descriptors is not None:
        for submodel_descriptor in submodel_descriptors:
            submodel_endpoint = Endpoint(
                interface="AAS-3.0",
                protocol_information=ProtocolInformation(
                    href=f"{get_base_url_submodel_repo()}/submodels/" + encode_id( submodel_descriptor.id ),
                    endpoint_protocol="http"
                )
            )
            submodel_descriptor.endpoints = [submodel_endpoint]
    return submodel_descriptors


def convert_shell_descriptor_endpoints_to_local_href(shell_descriptors: List[AssetAdministrationShellDescriptor]):
    for descriptor in shell_descriptors:
        endpoint = Endpoint(
            interface="AAS-3.0",
            protocol_information=ProtocolInformation(
                href=f"{get_base_url_shell_repo()}/shells/" + encode_id( descriptor.id ),
                endpoint_protocol="http"
            )
        )
        descriptor.endpoints = [endpoint]
        descriptor.submodel_descriptors = convert_submodel_descriptor_endpoints_to_local_href(descriptor.submodel_descriptors)

    return shell_descriptors

def convert_client_object_to_basyx_object(obj: object):
    obj_dict = api_client.sanitize_for_serialization(obj)
    shell_json = json.dumps(obj_dict)
    return json.loads(shell_json, cls=json_deserialization.AASFromJsonDecoder)

def to_camel_case(snake_str):
    """Convert snake_case string to camelCase."""
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def convert_dict_keys_to_camel_case(input_data):
    """Recursively convert dictionary keys to camelCase, handling lists as well."""
    if isinstance(input_data, dict):
        new_dict = {}
        for key, value in input_data.items():
            new_key = to_camel_case(key)  # Convert the key to camelCase
            new_dict[new_key] = convert_dict_keys_to_camel_case(value)  # Recursively convert the value
        return new_dict
    elif isinstance(input_data, list):
        return [convert_dict_keys_to_camel_case(item) for item in input_data]  # Process each item in the list
    else:
        return input_data  # Return the value if it's neither a dict nor a list


def get_paged_result_object(result: List, cursor: str, convert_to_camel_case: bool = False) -> dict:
    if cursor is None:
        paging_metadata = {}
    else:
        paging_metadata = {
            "cursor": cursor,
        }

    if convert_to_camel_case:
        r = convert_to_camel_case(result)
    else:
        r = result

    return {
        "paging_metadata": paging_metadata,
        "result": r
    }

def get_paged_result_json(result: List, cursor: str, convert_to_camel_case: bool = False) -> str:
    return json.dumps(
        get_paged_result_object(
            result,
            cursor,
            convert_to_camel_case
        )
    )