from datetime import datetime
from typing import Dict, Optional

from pydantic import BaseModel, Field


class SourceRecord(BaseModel):
    """Raw record from a simulated upstream database."""

    id: str
    source: str
    payload: Dict[str, str]
    extracted_at: datetime = Field(default_factory=datetime.utcnow)


class TransformedRecord(BaseModel):
    """Normalized record ready for the warehouse."""

    id: str
    source: str
    payload: Dict[str, str]
    ingested_at: datetime = Field(default_factory=datetime.utcnow)
    cache_shard: Optional[str] = None


class ETLRunResult(BaseModel):
    started_at: datetime
    finished_at: datetime
    records_extracted: int
    records_loaded: int
    cache_distribution: Dict[str, int]

