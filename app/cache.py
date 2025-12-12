import bisect
import hashlib
from typing import Dict, Iterable, List


def _hash(value: str) -> int:
    return int(hashlib.md5(value.encode("utf-8")).hexdigest(), 16)


class ConsistentHashRing:
    """Basic consistent hashing ring with virtual nodes."""

    def __init__(self, nodes: Iterable[str], virtual_nodes: int = 50):
        self.virtual_nodes = max(1, virtual_nodes)
        self.ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []
        for node in nodes:
            self.add_node(node)

    def add_node(self, node: str) -> None:
        for i in range(self.virtual_nodes):
            key = _hash(f"{node}-{i}")
            self.ring[key] = node
            bisect.insort(self.sorted_keys, key)

    def remove_node(self, node: str) -> None:
        keys_to_remove = [k for k, v in self.ring.items() if v == node]
        for key in keys_to_remove:
            self.ring.pop(key, None)
            idx = bisect.bisect_left(self.sorted_keys, key)
            if idx < len(self.sorted_keys) and self.sorted_keys[idx] == key:
                self.sorted_keys.pop(idx)

    def assign(self, key: str) -> str:
        if not self.ring:
            raise RuntimeError("Ring is empty")
        hash_key = _hash(key)
        idx = bisect.bisect(self.sorted_keys, hash_key)
        if idx == len(self.sorted_keys):
            idx = 0
        ring_key = self.sorted_keys[idx]
        return self.ring[ring_key]

    def distribution(self, keys: Iterable[str]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for key in keys:
            node = self.assign(key)
            counts[node] = counts.get(node, 0) + 1
        return counts


class ModuloSharder:
    """Simple key modulo sharding for comparison."""

    def __init__(self, nodes: Iterable[str]):
        self.nodes = list(nodes)

    def assign(self, key: str) -> str:
        if not self.nodes:
            raise RuntimeError("No nodes configured")
        return self.nodes[_hash(key) % len(self.nodes)]

    def distribution(self, keys: Iterable[str]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for key in keys:
            node = self.assign(key)
            counts[node] = counts.get(node, 0) + 1
        return counts

