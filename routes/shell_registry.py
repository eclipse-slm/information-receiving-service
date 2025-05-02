from fastapi import APIRouter, Response, HTTPException

from routes.routes_utils import decode_id
from routes.submodel_registry import submodel_descriptor_handler
from services.aas_utils import get_paged_result_json
from services.shell_descriptor_handler import ShellDescriptorHandler

router = APIRouter(prefix="/api/shell_registry", tags=["shell_registry"])

# in_memory_store = InMemoryStore()
shell_descriptor_handler = ShellDescriptorHandler()

@router.get(path="/shell-descriptors", status_code=200, description="Returns all Asset Administration Shell Descriptors")
def get_asset_administration_shell_descriptors(limit: int = 500, cursor: str = "0", aas_server_name: str = None):
    #
    # edc_descriptors = get_remote_shell_descriptors()
    # edc_descriptors = convert_shell_descriptor_endpoints_to_local_href(edc_descriptors)
    # edc_descriptors_dict = [descriptor.to_dict() for descriptor in edc_descriptors]
    #

    # for aas_server in config.aas_servers:
    #     remote_descriptors.extend(aas_server.shell_descriptors)
    # remote_descriptors_dict = [remote_descriptor.to_dict() for remote_descriptor in remote_descriptors]

    # return get_paged_result_object(remote_descriptors_dict)
    # remote_descriptors = []
    # all_rows = couch_db_client.db.all(as_list=True)
    # for i in range(cursor, limit):
    #     try:
    #         row = all_rows[i]
    #         remote_descriptors.append(
    #             AssetAdministrationShellDescriptor(**row['doc']['data'])
    #         )
    #     except IndexError:
    #         break;
    shell_descriptors, cursor = shell_descriptor_handler.get_shell_descriptors_by_aas_server_name(
        aas_server_name,
        limit,
        cursor
    )
    return Response(
        content=get_paged_result_json(shell_descriptors, cursor),
        media_type="application/json"
    )

@router.get(path="/shell-descriptors/{aasIdentifier}", status_code=200, description="Returns all Asset Administration Shell Descriptors")
def get_asset_administration_shell_descriptor(aasIdentifier: str):
    # descriptors = get_remote_shell_descriptors()
    # descriptor = [descriptor for descriptor in descriptors if descriptor.id == decode_id(aasIdentifier)]

    decoded_id = decode_id(aasIdentifier)

    descriptor = submodel_descriptor_handler.shell_descriptor(decoded_id)

    # if len(descriptor) == 0:
    #     LOG.warn("No shell descriptor found for id: " + aasIdentifier)
    #     return None
    # if len(descriptor) > 1:
    #     LOG.warn("Multiple shell descriptors found for id: " + aasIdentifier)

    if descriptor is None:
        raise HTTPException(status_code=404, detail="Shell descriptor not found")

    return descriptor

@router.get(path="/shell-descriptors/{aasIdentifier}/submodel-descriptors", status_code=200, description="Returns all Submodel Descriptors")
def get_submodel_descriptors(aasIdentifier: str):
    # descriptors = get_remote_shell_descriptors()
    # descriptor = [descriptor for descriptor in descriptors if descriptor.id == decode_id(aasIdentifier)]
    #
    # if len(descriptor) == 0:
    #     LOG.warn("No shell descriptor found for id: " + aasIdentifier)
    #     return None
    # if len(descriptor) > 1:
    #     LOG.warn("Multiple shell descriptors found for id: " + aasIdentifier)

    decoded_id = decode_id(aasIdentifier)
    descriptor = submodel_descriptor_handler.shell_descriptor(decoded_id)

    if descriptor is None:
        raise HTTPException(status_code=404, detail="Shell descriptor not found")

    return descriptor['submodelDescriptors']

@router.get(path="/shell-descriptors/{aasIdentifier}/submodel-descriptors/{submodelIdentifier}", status_code=200, description="Returns a specific Submodel Descriptor")
def get_submodel_descriptor(aasIdentifier: str, submodelIdentifier: str):
    # shell_descriptors = get_remote_shell_descriptors()
    # shell_descriptor = [descriptor for descriptor in shell_descriptors if descriptor.id == decode_id(aasIdentifier)]
    #
    # if len(shell_descriptor) == 0:
    #     LOG.warn("No shell descriptor found for id: " + aasIdentifier)
    #     return None
    # if len(shell_descriptor) > 1:
    #     LOG.warn("Multiple shell descriptors found for id: " + aasIdentifier)
    #
    # submodel_descriptors = shell_descriptor[0].submodel_descriptors
    # submodel_descriptor = [submodel_descriptor for submodel_descriptor in submodel_descriptors if submodel_descriptor.id == decode_id(submodelIdentifier)]
    #
    # if len(submodel_descriptor) == 0:
    #     LOG.warn("No submodel descriptor found for id: " + submodelIdentifier)
    #     return None
    # if len(submodel_descriptor) > 1:
    #     LOG.warn("Multiple submodel descriptors found for id: " + submodelIdentifier)

    # return submodel_descriptor[0].to_dict()

    decoded_aas_id = decode_id(aasIdentifier)
    decoded_sm_id = decode_id(submodelIdentifier)
    descriptor = submodel_descriptor_handler.shell_descriptor(decoded_aas_id)

    if descriptor is None:
        raise HTTPException(status_code=404, detail="Shell descriptor not found")

    for submodel_descriptor in descriptor['submodelDescriptors']:
        if submodel_descriptor['id'] == decoded_sm_id:
            return submodel_descriptor

    raise HTTPException(status_code=404, detail="Submodel descriptor not found")

