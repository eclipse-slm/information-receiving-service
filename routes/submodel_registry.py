from aas_python_http_client import SubmodelDescriptor, ApiClient
from fastapi import APIRouter, HTTPException
from starlette.responses import Response

from services.aas_utils import decode_id
from routes.shell_registry import in_memory_store
from services.aas_utils import get_paged_result_json
from services.in_memory_store.in_memory_store import InMemoryStore

router = APIRouter(prefix="/api/submodel_registry", tags=["submodel_registry"])

in_memory_store = InMemoryStore()

@router.get(path="/submodel-descriptors", status_code=200, description="Returns all Submodel Descriptors")
def get_submodel_descriptors(aas_server_name: str = None):
    # descriptors = get_remote_shell_descriptors()
    # submodel_descriptors = []
    #
    # for descriptor in descriptors:
    #     if descriptor.submodel_descriptors is not None:
    #         submodel_descriptors.extend(descriptor.submodel_descriptors)
    #
    # submodel_descriptors = convert_submodel_descriptor_endpoints_to_local_href(submodel_descriptors)
    #
    # return get_paged_result_object(
    #     [submodel_descriptor.to_dict() for submodel_descriptor in submodel_descriptors]
    # )
    submodel_descriptors = in_memory_store.get_submodel_descriptors_by_aas_server_name(aas_server_name)

    return Response(
        content=get_paged_result_json(submodel_descriptors, None),
        media_type="application/json"
    )

@router.get(path="/submodel-descriptors/{submodelIdentifier}", status_code=200, description="Return a specific Submodel Descriptor")
def get_submodel_descriptor(submodelIdentifier: str):
    # descriptors = get_remote_shell_descriptors()
    # submodel_descriptors = []
    #
    # for descriptor in descriptors:
    #     if descriptor.submodel_descriptors is not None:
    #         submodel_descriptors.extend(descriptor.submodel_descriptors)
    #
    # submodel_descriptors = [submodel_descriptor for submodel_descriptor in submodel_descriptors if submodel_descriptor.id == decode_id(submodelIdentifier)]
    #
    # if len(submodel_descriptors) == 0:
    #     LOG.warn("No submodel descriptor found for id: " + submodelIdentifier)
    #     return None
    # if len(submodel_descriptors) > 1:
    #     LOG.warn("Multiple submodel descriptors found for id: " + submodelIdentifier)
    #
    # submodel_descriptors = convert_submodel_descriptor_endpoints_to_local_href(submodel_descriptors)
    # submodel_descriptor = submodel_descriptors[0]
    #
    # return convert_dict_keys_to_camel_case(
    #     submodel_descriptor.to_dict()
    # )
    id = decode_id(submodelIdentifier)

    submodel_descriptor = in_memory_store.submodel_descriptor(id)

    if submodel_descriptor is None:
        raise HTTPException(status_code=404, detail="Submodel descriptor not found")

    return submodel_descriptor
