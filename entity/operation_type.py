from dataclasses import dataclass
from typing import Iterable, Optional, Tuple


def _normalize_scalar(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _dedupe_preserve(sequence: Iterable[Optional[str]]) -> Tuple[str, ...]:
    seen = set()
    ordered = []
    for item in sequence:
        if not item:
            continue
        if item in seen:
            continue
        ordered.append(item)
        seen.add(item)
    return tuple(ordered)


@dataclass(frozen=True)
class OperationType:
    type_name: Optional[str]
    subtype_name: Optional[str]
    chain: Tuple[str, ...]

    OPERATION_LABELS = ("__Operation__", "Operation")
    TYPE_LABEL = "OperationType"

    @classmethod
    def from_payload(cls, payload: Optional[dict]) -> "OperationType":
        source = payload or {}
        type_name = _normalize_scalar(source.get("Type"))
        subtype_name = _normalize_scalar(source.get("SubType"))
        explicit_chain = cls._normalize_chain(source.get("TypeChain"))
        chain = cls._build_chain(type_name, subtype_name, explicit_chain)
        return cls(type_name=type_name, subtype_name=subtype_name, chain=chain)

    @staticmethod
    def _normalize_chain(raw: object) -> Tuple[str, ...]:
        if isinstance(raw, (list, tuple)):
            return _dedupe_preserve(_normalize_scalar(item) for item in raw)
        return ()

    @classmethod
    def _build_chain(
        cls,
        type_name: Optional[str],
        subtype_name: Optional[str],
        explicit_chain: Tuple[str, ...],
    ) -> Tuple[str, ...]:
        if explicit_chain:
            return explicit_chain
        if subtype_name:
            prefixed = f"__{type_name}__" if type_name else None
            return _dedupe_preserve((prefixed, subtype_name))
        if type_name:
            return _dedupe_preserve((type_name,))
        return ()

    @property
    def operation_labels(self) -> Tuple[str, ...]:
        base = list(self.OPERATION_LABELS)
        labels = _dedupe_preserve(base + list(self.chain))
        return labels or tuple(base)

    @property
    def type_properties(self) -> dict:
        props: dict = {}
        name = self.subtype_name or self.type_name or "UnknownOperationType"
        props["Name"] = name
        if self.type_name:
            props["Type"] = self.type_name
        if self.subtype_name:
            props["SubType"] = self.subtype_name
        if self.chain:
            props["TypeChain"] = list(self.chain)
        return props

    def apply_operation_properties(self, properties: dict) -> None:
        if self.chain and "TypeChain" not in properties:
            properties["TypeChain"] = list(self.chain)
        if self.type_name and "Type" not in properties:
            properties["Type"] = self.type_name
        if self.subtype_name and "SubType" not in properties:
            properties["SubType"] = self.subtype_name
