
from dataclasses import dataclass, field
from typing import Dict, Iterable, Set


@dataclass
class Process:
    index: int
    payload: Dict[str, object] = field(default_factory=dict)
    neighbors: Set[int] = field(default_factory=set)
    features: Set[int] = field(default_factory=set)

    def set_payload(self, process_payload: dict) -> None:
        if not isinstance(process_payload, dict):
            return
        if self.payload:
            for key, value in process_payload.items():
                self.payload.setdefault(key, value)
        else:
            self.payload = dict(process_payload)

    def add_neighbor(self, neighbor_index: int) -> None:
        if neighbor_index == self.index:
            return
        self.neighbors.add(neighbor_index)

    def add_feature(self, feature_index: int) -> None:
        self.features.add(feature_index)

    @classmethod
    def build_graph(
        cls,
        processes: Iterable[dict],
        process_index: Iterable[Iterable[int]],
        feature_process_map: Dict[str, int],
    ) -> Dict[int, "Process"]:
        nodes: Dict[int, Process] = {}

        for process in processes or []:
            if not isinstance(process, dict):
                continue
            raw_index = process.get("index")
            try:
                idx = int(raw_index)
            except (TypeError, ValueError):
                continue

            node = nodes.setdefault(idx, cls(index=idx))
            node.set_payload(process)

        for pair in process_index or []:
            if not isinstance(pair, Iterable):
                continue
            try:
                lhs, rhs = list(pair)[:2]
            except (TypeError, ValueError):
                continue

            try:
                left_idx = int(lhs)
                right_idx = int(rhs)
            except (TypeError, ValueError):
                continue

            left_node = nodes.setdefault(left_idx, cls(index=left_idx))
            right_node = nodes.setdefault(right_idx, cls(index=right_idx))
            left_node.add_neighbor(right_idx)
            right_node.add_neighbor(left_idx)

        for feature_key, raw_process_idx in (feature_process_map or {}).items():
            try:
                feature_idx = int(feature_key)
                process_idx = int(raw_process_idx)
            except (TypeError, ValueError):
                continue

            node = nodes.setdefault(process_idx, cls(index=process_idx))
            node.add_feature(feature_idx)

        return nodes

