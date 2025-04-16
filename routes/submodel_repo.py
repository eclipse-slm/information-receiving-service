import binascii

from fastapi import APIRouter, HTTPException
from starlette.responses import Response

from routes.routes_utils import decode_id
from services.aas_utils import get_paged_result_json
from services.in_memory_store.in_memory_store import InMemoryStore
from services.submodel_handler import SubmodelHandler

router = APIRouter(prefix="/api/submodel_repo", tags=["submodel_repo"])

in_memory_store = InMemoryStore()
submodel_handler = SubmodelHandler()

@router.get(path="/submodels", status_code=200, description="Returns all Submodels")
def get_submodels(limit: int = 100, cursor: str = "0", aas_server_name: str = None):
    #return json.loads(json.dumps(aas.get_submodels(cached), cls=AASToJsonEncoder))
    submodels, cursor = in_memory_store.get_submodels_by_aas_server_name(
        aas_server_name,
        limit,
        cursor
    )

    return Response(
        content=get_paged_result_json(submodels, cursor),
        media_type="application/json"
    )

@router.get(path="/submodels/$value", status_code=200, description="Returns all Submodels")
def get_submodels_value_only(limit: int = 100, cursor: str = "0", aas_server_name: str = None):
    #return json.loads(json.dumps(aas.get_submodels(cached), cls=AASToJsonEncoder))
    submodels, cursor = in_memory_store.get_submodels_by_aas_server_name(
        aas_server_name,
        limit,
        cursor
    )
    submodels_value_only = submodel_handler.get_submodels_value_only(submodels)
    return Response(
        content=get_paged_result_json(submodels_value_only, cursor),
        media_type="application/json"
    )

@router.get(path="/submodels/{submodelIdentifier}", status_code=200, description="Returns a specific Submodel")
def get_submodel(submodelIdentifier: str, cached: bool = True):
    decoded_id = decode_id(submodelIdentifier)

    # if cached:
    #     submodel = get_cached_submodel(decode_id(submodelIdentifier))
    # else:
    #     submodel = get_remote_submodel(decode_id(submodelIdentifier))
    #
    # return json.loads(json.dumps(submodel, cls=AASToJsonEncoder))
    submodel = submodel_handler.get_submodel(decoded_id)

    if submodel is None:
        raise HTTPException(status_code=404, detail="Submodel not found")

    return submodel

@router.get(path="/submodels/{submodelIdentifier}/$value", status_code=200, description="Returns a specific Submodel in value format")
def get_submodel_value_only(submodelIdentifier: str):
    decoded_id = decode_id(submodelIdentifier)
    submodel = submodel_handler.get_submodel_value_only(decoded_id)

    if submodel is None:
        raise HTTPException(status_code=404, detail="Submodel not found")

    return submodel


# @router.get(path="/submodels/{submodelIdentifier}/submodel-elements/{submodelElementIdentifier}", status_code=200, description="Returns a specific Submodel Element")
# def get_submodel_element(submodelIdentifier: str, submodelElementIdentifier: str, cached: bool = True):
#     if cached:
#         submodel = get_cached_submodel(decode_id(submodelIdentifier))
#     else:
#         submodel = get_remote_submodel(decode_id(submodelIdentifier))
#
#     sme = submodel.submodel_element.get(attribute_name="id_short", attribute_value=submodelElementIdentifier)
#
#     return json.loads(json.dumps(sme, cls=AASToJsonEncoder))
