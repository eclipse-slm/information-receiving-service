from typing import List

from fastapi import APIRouter

from services.aas_utils import decode_id
from services.shell_descriptor_handler import ShellDescriptorHandler

router = APIRouter(prefix="/api/shell_discovery", tags=["shell_discovery"])

shell_descriptor_handler = ShellDescriptorHandler()

@router.get(path="/lookup/shells", status_code=200, description="Returns a list of Asset Administration Shell ids linked to specific Asset identifiers")
def lookup_shells(assetId: str) -> List[str]:
    shell_ids = shell_descriptor_handler.get_shell_ids_by_asset_id(decode_id(assetId))
    return shell_ids