from pydantic import BaseModel
from datetime import datetime
from typing import List

class RangeRequest(BaseModel):
    start: datetime   # ISO 8601; si es naive, trátalo como UTC
    end:   datetime
    limit: int = 1000

class Dato(BaseModel):
    fecha: datetime
    valor: float

class RangeResponse(BaseModel):
    count: int
    items: List[Dato]
