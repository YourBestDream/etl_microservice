import random
import string
import uuid
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

from .models import SourceRecord


def _random_word(length: int = 6) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


class MockDatabase:
    """In-memory stand-in for an upstream database."""

    def __init__(self, name: str):
        self.name = name
        self._rows: List[SourceRecord] = []

    def seed(self, count: int) -> None:
        for _ in range(count):
            self.insert_random()

    def insert_random(self) -> SourceRecord:
        now = datetime.utcnow()
        record = SourceRecord(
            id=str(uuid.uuid4()),
            source=self.name,
            payload={
                "title": _random_word(),
                "category": random.choice(["alpha", "beta", "gamma"]),
            },
            extracted_at=now - timedelta(seconds=random.randint(0, 300)),
        )
        self._rows.append(record)
        return record

    def add(self, payload: Dict[str, str]) -> SourceRecord:
        record = SourceRecord(
            id=str(uuid.uuid4()),
            source=self.name,
            payload=payload,
            extracted_at=datetime.utcnow(),
        )
        self._rows.append(record)
        return record

    def fetch_since(self, since: Optional[datetime]) -> List[SourceRecord]:
        if since is None:
            return list(self._rows)
        return [row for row in self._rows if row.extracted_at > since]

    def __len__(self) -> int:
        return len(self._rows)


class SourceRegistry:
    """Collection of mocked databases to extract from."""

    def __init__(self, source_names: Iterable[str], seed_records: int):
        self.sources: Dict[str, MockDatabase] = {
            name: MockDatabase(name) for name in source_names
        }
        for source in self.sources.values():
            source.seed(seed_records)

    def fetch_all_since(self, since: Optional[datetime]) -> List[SourceRecord]:
        rows: List[SourceRecord] = []
        for source in self.sources.values():
            rows.extend(source.fetch_since(since))
        return rows

    def add_to_source(self, source: str, payload: Dict[str, str]) -> SourceRecord:
        if source not in self.sources:
            raise KeyError(f"Unknown source '{source}'")
        return self.sources[source].add(payload)

    def stats(self) -> Dict[str, int]:
        return {name: len(src) for name, src in self.sources.items()}

