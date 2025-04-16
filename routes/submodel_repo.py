import binascii

from fastapi import APIRouter, HTTPException

from services.aas_utils import decode_id
from services.in_memory_store.in_memory_store import InMemoryStore
from services.submodel_handler import SubmodelHandler

router = APIRouter(prefix="/api/submodel_repo", tags=["submodel_repo"])

in_memory_store = InMemoryStore()
submodel_handler = SubmodelHandler()

# @router.get(path="/submodels", status_code=200, description="Returns all Submodels")
# def get_submodels(cached: bool = True):
#     #return json.loads(json.dumps(aas.get_submodels(cached), cls=AASToJsonEncoder))
#     return Response(
#         content=get_paged_result_json(in_memory_store.submodels),
#         media_type="application/json"
#     )

@router.get(path="/submodels/{submodelIdentifier}", status_code=200, description="Returns a specific Submodel")
def get_submodel(submodelIdentifier: str, cached: bool = True):
    try:
        decoded_id = decode_id(submodelIdentifier)
    except binascii.Error as e:
        raise HTTPException(status_code=404, detail="Item not found")

    # if cached:
    #     submodel = get_cached_submodel(decode_id(submodelIdentifier))
    # else:
    #     submodel = get_remote_submodel(decode_id(submodelIdentifier))
    #
    # return json.loads(json.dumps(submodel, cls=AASToJsonEncoder))
    submodel = submodel_handler.get_submodel(decoded_id)

    if submodel is None:
        raise HTTPException(status_code=404, detail="Item not found")

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
