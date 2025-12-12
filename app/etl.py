from datetime import datetime
from typing import Dict, Iterable, List, Optional

from .cache import ConsistentHashRing, ModuloSharder
from .models import ETLRunResult, SourceRecord, TransformedRecord
from .simulated_sources import SourceRegistry
from .warehouse import Warehouse


class ETLPipeline:
    """Basic extract-transform-load pipeline against mocked data sources."""

    def __init__(
        self,
        sources: SourceRegistry,
        warehouse: Warehouse,
        cache_nodes: Iterable[str],
        cache_virtual_nodes: int,
    ):
        self.sources = sources
        self.warehouse = warehouse
        self.last_run_at: Optional[datetime] = None
        self.last_result: Optional[ETLRunResult] = None

        self.consistent_hash = ConsistentHashRing(
            cache_nodes, virtual_nodes=cache_virtual_nodes
        )
        self.modulo_sharder = ModuloSharder(cache_nodes)

    async def run(self) -> ETLRunResult:
        started_at = datetime.utcnow()
        raw_records = self.sources.fetch_all_since(self.last_run_at)
        transformed = [self._transform(record) for record in raw_records]
        loaded = self.warehouse.load(transformed)
        finished_at = datetime.utcnow()

        distribution = self.consistent_hash.distribution([rec.id for rec in transformed])

        self.last_run_at = finished_at
        self.last_result = ETLRunResult(
            started_at=started_at,
            finished_at=finished_at,
            records_extracted=len(raw_records),
            records_loaded=loaded,
            cache_distribution=distribution,
        )
        return self.last_result

    def _transform(self, record: SourceRecord) -> TransformedRecord:
        shard = self.consistent_hash.assign(record.id)
        clean_payload = dict(record.payload)
        clean_payload["origin"] = record.source
        clean_payload["normalized_title"] = clean_payload.get("title", "").title()
        return TransformedRecord(
            id=record.id,
            source=record.source,
            payload=clean_payload,
            cache_shard=shard,
        )

    def sharding_comparison(self, keys: Iterable[str]) -> Dict[str, Dict[str, int]]:
        return {
            "consistent_hash": self.consistent_hash.distribution(keys),
            "modulo": self.modulo_sharder.distribution(keys),
        }
