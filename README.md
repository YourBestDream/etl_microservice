# ETL microservice (FastAPI)

Mocked ETL pipeline that extracts data from simulated databases, transforms it, and loads it into an in-memory warehouse. Includes a small consistent-hashing vs modulo sharding comparison for the caching task.

## Quickstart

```bash
python -m venv .venv
. .venv/Scripts/activate   # PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Open http://127.0.0.1:8000/docs for the interactive API.

### Warehouse backends

- `WAREHOUSE_BACKEND=memory` (default) keeps data in RAM only.
- `WAREHOUSE_BACKEND=sqlite` writes to `WAREHOUSE_PATH` (defaults to `data/warehouse.db`) to mimic a persistent warehouse; replicas are materialized as extra tables in the same file.

Set these as environment variables before launching Uvicorn.

### Docker

```bash
docker build -t etl-microservice .
docker run --rm -p 8000:8000 -e WAREHOUSE_BACKEND=sqlite etl-microservice
```

Or with Compose (persists the SQLite file under `./data`):

```bash
docker-compose up --build
```

## API

- `GET /health` – liveness check.
- `GET /sources/stats` – number of mocked rows per source.
- `POST /sources/{source}/seed` – add a record to a mocked source.
- `POST /etl/run` (`?async_mode=true` optional) – trigger ETL extraction + load.
- `GET /etl/status` – last run metadata.
- `GET /warehouse/snapshot` – recent warehouse rows + replication metrics.
- `POST /cache/shards` – supply keys to see consistent hashing vs modulo shard distribution.

## What is mocked

- Upstream databases are represented by in-memory `MockDatabase` instances seeded with sample data.
- The data warehouse is an in-memory store with N replicas to illustrate redundancy/failover.
- Cache sharding uses a consistent hash ring with virtual nodes and a modulo sharder for comparison.

This keeps the service runnable without external databases or cache servers while demonstrating the required behaviors.
