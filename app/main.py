import asyncio

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .config import settings
from .etl import ETLPipeline
from .models import ETLRunResult
from .simulated_sources import SourceRegistry
from .warehouse import create_warehouse


class SeedPayload(BaseModel):
    payload: dict


class ShardRequest(BaseModel):
    keys: list[str]


app = FastAPI(
    title="ETL Microservice",
    version="0.1.0",
    description="FastAPI ETL microservice with mocked databases and sharding demo.",
)

sources = SourceRegistry(settings.source_names, settings.default_seed_records)
warehouse = create_warehouse(
    backend=settings.warehouse_backend,
    replica_count=settings.replica_count,
    path=settings.warehouse_path,
)
pipeline = ETLPipeline(
    sources,
    warehouse,
    cache_nodes=settings.cache_nodes,
    cache_virtual_nodes=settings.cache_virtual_nodes,
)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/sources/stats")
async def source_stats() -> dict:
    return {"sources": sources.stats()}


@app.post("/sources/{source}/seed")
async def seed_source(source: str, payload: SeedPayload) -> dict:
    try:
        record = sources.add_to_source(source, payload.payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"inserted": record}


async def _run_etl() -> ETLRunResult:
    return await pipeline.run()


@app.post("/etl/run")
async def etl_run(async_mode: bool = False) -> dict:
    if async_mode:
        asyncio.create_task(_run_etl())
        return {"status": "scheduled"}
    result = await _run_etl()
    return {"status": "completed", "result": result}


@app.get("/etl/status")
async def etl_status() -> dict:
    if pipeline.last_result:
        return {"last_run": pipeline.last_result}
    return {"last_run": None}


@app.get("/warehouse/snapshot")
async def warehouse_snapshot(limit: int = 20) -> dict:
    return {
        "rows": warehouse.snapshot(limit),
        "metrics": warehouse.metrics(),
    }


@app.post("/cache/shards")
async def cache_shard_distribution(request: ShardRequest) -> dict:
    if not request.keys:
        raise HTTPException(status_code=400, detail="No keys provided")
    comparison = pipeline.sharding_comparison(request.keys)
    return {"comparison": comparison}
