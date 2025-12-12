import json
import os
import sqlite3
from datetime import datetime
from typing import Dict, Iterable, List, Protocol

from .models import TransformedRecord


class Warehouse(Protocol):
    def load(self, records: Iterable[TransformedRecord]) -> int:
        ...

    def snapshot(self, limit: int = 50) -> List[TransformedRecord]:
        ...

    def metrics(self) -> Dict[str, int]:
        ...


class InMemoryWarehouse:
    """Lightweight in-memory warehouse with naive replication."""

    def __init__(self, replica_count: int):
        self.primary: List[TransformedRecord] = []
        self.replicas: List[List[TransformedRecord]] = [
            [] for _ in range(max(replica_count, 0))
        ]

    def load(self, records: Iterable[TransformedRecord]) -> int:
        loaded = 0
        for record in records:
            self.primary.append(record)
            for replica in self.replicas:
                replica.append(record)
            loaded += 1
        return loaded

    def snapshot(self, limit: int = 50) -> List[TransformedRecord]:
        return self.primary[-limit:]

    def metrics(self) -> Dict[str, int]:
        return {
            "backend": "memory",
            "primary_rows": len(self.primary),
            "replica_rows_each": len(self.replicas[0]) if self.replicas else 0,
            "replica_count": len(self.replicas),
        }


class SQLiteWarehouse:
    """SQLite-backed warehouse to mimic a persistent data warehouse."""

    def __init__(self, db_path: str, replica_count: int):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.tables = ["warehouse"] + [
            f"warehouse_replica_{i+1}" for i in range(max(replica_count, 0))
        ]
        for table in self.tables:
            self._init_table(table)

    def _init_table(self, table: str) -> None:
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                payload TEXT NOT NULL,
                ingested_at TEXT NOT NULL,
                cache_shard TEXT
            );
            """
        )
        self.conn.commit()

    def _insert(self, table: str, record: TransformedRecord) -> None:
        self.conn.execute(
            f"""
            INSERT OR REPLACE INTO {table} (id, source, payload, ingested_at, cache_shard)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                record.id,
                record.source,
                json.dumps(record.payload),
                record.ingested_at.isoformat(),
                record.cache_shard,
            ),
        )

    def load(self, records: Iterable[TransformedRecord]) -> int:
        loaded = 0
        for record in records:
            for table in self.tables:
                self._insert(table, record)
            loaded += 1
        self.conn.commit()
        return loaded

    def snapshot(self, limit: int = 50) -> List[TransformedRecord]:
        rows = self.conn.execute(
            """
            SELECT id, source, payload, ingested_at, cache_shard
            FROM warehouse
            ORDER BY ingested_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        result: List[TransformedRecord] = []
        for row in rows:
            result.append(
                TransformedRecord(
                    id=row["id"],
                    source=row["source"],
                    payload=json.loads(row["payload"]),
                    ingested_at=datetime.fromisoformat(row["ingested_at"]),
                    cache_shard=row["cache_shard"],
                )
            )
        return result

    def metrics(self) -> Dict[str, int]:
        counts = {}
        for table in self.tables:
            row = self.conn.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()
            counts[table] = row["c"]
        return {
            "backend": "sqlite",
            "primary_rows": counts.get("warehouse", 0),
            "replica_count": len(self.tables) - 1,
            "replica_rows_each": counts.get("warehouse_replica_1", 0)
            if len(self.tables) > 1
            else 0,
        }


def create_warehouse(backend: str, replica_count: int, path: str) -> Warehouse:
    backend = backend.lower()
    if backend == "sqlite":
        return SQLiteWarehouse(path, replica_count)
    return InMemoryWarehouse(replica_count)
