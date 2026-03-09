from typing import Dict
from pydantic import BaseModel


class CoinPrice(BaseModel):
    usd: float


class RawPayload(BaseModel):
    ingested_at: float
    source: str
    raw_data: Dict[str, CoinPrice]
