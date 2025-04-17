import binascii

from fastapi import HTTPException

from services import aas_utils


def decode_id(id_enc: str) -> str:
    try:
        return aas_utils.decode_id(id_enc)
    except (binascii.Error, UnicodeDecodeError) as e:
        raise HTTPException(status_code=400, detail="ID no valid base64 string")