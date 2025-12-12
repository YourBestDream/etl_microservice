from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    """Runtime configuration for the ETL microservice."""

    source_names: List[str] = ["crm", "erp", "analytics"]
    cache_nodes: List[str] = ["cache-a", "cache-b", "cache-c"]
    cache_virtual_nodes: int = 50
    replica_count: int = 2
    default_seed_records: int = 5
    batch_size: int = 50
    warehouse_backend: str = "memory"  # options: memory, sqlite
    warehouse_path: str = "data/warehouse.db"


settings = Settings()
