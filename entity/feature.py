
from dataclasses import dataclass, field
from typing import Dict, Iterable, Set


@dataclass
class MachiningFeature:
    index: int
    surfaces: Set[str] = field(default_factory=set)
    neighbors: Set[int] = field(default_factory=set)

    def add_surface(self, surface_key: str) -> None:
        self.surfaces.add(surface_key)

    def add_neighbor(self, neighbor_index: int) -> None:
        if neighbor_index == self.index:
            return
        self.neighbors.add(neighbor_index)

    @classmethod
    def build_graph(
        cls,
        surface_feature_map: Dict[str, int],
        feature_index: Iterable[Iterable[int]],
    ) -> Dict[int, "MachiningFeature"]:
        features: Dict[int, MachiningFeature] = {}

        for face_key, raw_feature_idx in surface_feature_map.items():
            if raw_feature_idx is None:
                continue
            try:
                feature_idx = int(raw_feature_idx)
            except (TypeError, ValueError):
                continue

            feature = features.setdefault(feature_idx, cls(index=feature_idx))
            feature.add_surface(str(face_key))

        for pair in feature_index or []:
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

            left_feature = features.setdefault(left_idx, cls(index=left_idx))
            right_feature = features.setdefault(right_idx, cls(index=right_idx))
            left_feature.add_neighbor(right_idx)
            right_feature.add_neighbor(left_idx)

        return features

