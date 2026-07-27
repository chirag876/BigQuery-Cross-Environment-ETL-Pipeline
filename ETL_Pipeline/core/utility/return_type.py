from datetime import datetime
from json import JSONEncoder, dumps
from typing import Any
from uuid import UUID

from fastapi import Response


class Encoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, UUID):
            # if the o is uuid, we simply return the value of uuid
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return JSONEncoder.default(self, o)


def custom_response(content: Any, status_code: int, media_type: str):
    if not isinstance(content, str):
        content = dumps(content, cls=Encoder)
    return Response(content=content, status_code=status_code, media_type=media_type)


def json(content: Any, status_code: int = 200):
    return custom_response(
        content=content, status_code=status_code, media_type="application/json"
    )
