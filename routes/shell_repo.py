import json

from fastapi import APIRouter, HTTPException
from starlette.responses import Response

from routes.routes_utils import decode_id
from services.aas_utils import extract_submodel_references_from_shell_descriptor, get_paged_result_object, \
    get_paged_result_json
from services.in_memory_store.in_memory_store import InMemoryStore

router = APIRouter(prefix="/api/shell_repo", tags=["shell_repo"])

in_memory_store = InMemoryStore()

@router.get(path="/shells", status_code=200, description="Returns all Asset Administration Shells")
def get_asset_administration_shells(limit: int = 500, cursor: str = "0", aas_server_name: str = None):
    # shell_descriptors = get_remote_shell_descriptors()
    # shells = []
    #
    # if cached:
    #     for shell_descriptor in shell_descriptors:
    #         try:
    #             shells.append(
    #                 aas_obj_store.get_identifiable(shell_descriptor.id)
    #             )
    #         except KeyError as e:
    #             LOG.info("Unable to find shell in cache: " + str(e))
    #             new_shell = convert_shell_descriptors_to_shells([shell_descriptor])[0]
    #             aas_obj_store.add(
    #                 convert_client_object_to_basyx_object(new_shell)
    #             )
    #             shells.append(
    #                 aas_obj_store.get_identifiable(shell_descriptor.id)
    #             )
    #
    #     return json.loads(json.dumps(shells, cls=AASToJsonEncoder))
    # else:
    #     shells = convert_shell_descriptors_to_shells(shell_descriptors)
    #     return [shell.to_dict() for shell in shells]

    shells, cursor = in_memory_store.get_shells_by_aas_server_name(aas_server_name, limit, cursor)

    return Response(
        content=get_paged_result_json(shells, cursor),
        media_type="application/json"
    )

@router.get(path="/shells/{aasIdentifier}", status_code=200, description="Returns a specific Asset Administration Shells")
def get_asset_administration_shell(aasIdentifier: str, cached: bool = True):
    # aas_id_dec = aas_utils.decode_id(aasIdentifier)
    #
    # if cached:
    #     try:
    #         shell = aas_obj_store.get_identifiable(aas_id_dec)
    #     except KeyError as e:
    #         LOG.info("Unable to find shell in cache: " + str(e))
    #         shell_descriptor = get_remote_shell_descriptor(aas_id_dec)
    #         new_shell = convert_shell_descriptors_to_shells([shell_descriptor])[0]
    #         aas_obj_store.add(
    #             convert_client_object_to_basyx_object(new_shell)
    #         )
    #         AssetAdministrationShell
    #         shell = aas_obj_store.get_identifiable(shell_descriptor.id)
    #
    #     return json.loads(json.dumps(shell, cls=AASToJsonEncoder))
    # else:
    #     descriptor = get_remote_shell_descriptor(aas_id_dec)
    #     shell = convert_shell_descriptor_to_shell(descriptor)
    #     return shell.to_dict()
    aas_id_dec = decode_id(aasIdentifier)

    shell = in_memory_store.shell(identifier=aas_id_dec)
    if shell is None:
        raise HTTPException(status_code=404, detail="Shell not found")

    return Response(
        content=json.dumps(),
        media_type="application/json"
    )

@router.get(path="/shells/{aasIdentifier}/submodel-refs", status_code=200, description="Returns all submodel references")
def get_asset_administration_shell(aasIdentifier: str):
    # aas_id_dec = aas_utils.decode_id(aasIdentifier)
    # descriptor = get_remote_shell_descriptor(aas_id_dec)

    aas_id_dec = decode_id(aasIdentifier)
    shell_descriptor = in_memory_store.shell_descriptor(identifier=aas_id_dec)
    if shell_descriptor is None:
        raise HTTPException(status_code=404, detail="Related shell descriptor not found")

    submodel_references = extract_submodel_references_from_shell_descriptor(shell_descriptor)
    submodel_references_dict = [ref.to_dict() for ref in submodel_references]

    return get_paged_result_object(
        submodel_references_dict,
        None
    )