
import json
import pathlib
from collections.abc import Iterable
from typing import Any, Dict, List, Tuple

from utils.hash import file_hash
from utils.neo4j import connect_neo4j


def _as_float_list(raw_values: Iterable[Any]) -> List[float]:
    result: List[float] = []
    for value in raw_values:
        try:
            result.append(float(value))
        except (TypeError, ValueError):
            raise ValueError("Embedding values must be numeric.") from None
    if not result:
        raise ValueError("Embedding list cannot be empty.")
    return result


def _write_surface_embedding(
    tx,
    file_id: str,
    surface_index: str,
    embedding: List[float],
    predict_value: Any,
) -> int:
    result = tx.run(
        """
        MATCH (surface:Surface {__fileId__: $file_id, __index__: $surface_index})
        SET surface.embedding = $embedding,
            surface.predict = $predict_value,
            surface.embeddingDim = size($embedding)
        RETURN count(surface) AS updated
        """,
        file_id=file_id,
        surface_index=surface_index,
        embedding=embedding,
        predict_value=predict_value,
    )
    record = result.single()
    return int(record["updated"]) if record else 0


def _iter_embedding_payloads(raw_payload: Dict[str, Any]) -> Iterable[Tuple[str, Dict[str, Any]]]:
    for key, value in raw_payload.items():
        if not isinstance(value, dict):
            continue
        yield str(key), value


def _average_vectors(vectors: List[List[float]]) -> List[float]:
    if not vectors:
        raise ValueError("Cannot average empty vector collection.")

    dimension = len(vectors[0])
    if not dimension:
        raise ValueError("Vectors must not be empty.")

    for vector in vectors:
        if len(vector) != dimension:
            raise ValueError("Vectors must share the same dimension.")

    return [sum(values) / len(vectors) for values in zip(*vectors)]


def _write_feature_embedding(
    tx,
    file_id: str,
    feature_index: int,
    embedding: List[float],
    contributing_surfaces: int,
) -> int:
    result = tx.run(
        """
        MATCH (feature:MachiningFeature {__fileId__: $file_id, __index__: $feature_index})
        SET feature.embedding = $embedding,
            feature.embeddingDim = size($embedding),
            feature.embeddingSurfaceCount = $contributing_surfaces
        RETURN count(feature) AS updated
        """,
        file_id=file_id,
        feature_index=feature_index,
        embedding=embedding,
        contributing_surfaces=contributing_surfaces,
    )
    record = result.single()
    return int(record["updated"]) if record else 0


if __name__ == "__main__":

    init = False
    driver = connect_neo4j(init=init)

    hashfile = r"E:\dataset\cam\260108test\process_graph\cs1.stp"
    hash_value = file_hash(hashfile)

    json_file = pathlib.Path(r"E:\dataset\cam\260108test\embedding\cs1.json")
    with open(json_file, "r", encoding="utf-8") as file:
        payload = json.load(file)

    pyg_json_file = pathlib.Path(r"E:\dataset\cam\260108test\mynet_multi_kgv2\cs1.json")
    with open(pyg_json_file, "r", encoding="utf-8") as file:
        pyg_payload = json.load(file)

    surface_embeddings: Dict[str, List[float]] = {}
    with driver.session() as session:
        total = 0
        missing = 0
        for surface_index, data in _iter_embedding_payloads(payload):
            embedding_raw = data.get("embedding")
            if not isinstance(embedding_raw, Iterable) or isinstance(embedding_raw, (str, bytes)):
                print(f"Skip surface {surface_index}: embedding must be a sequence of numbers.")
                continue

            try:
                embedding = _as_float_list(embedding_raw)
            except ValueError as exc:
                print(f"Skip surface {surface_index}: {exc}")
                continue

            predict_value = data.get("predict")
            updated = session.execute_write(
                _write_surface_embedding,
                hash_value,
                surface_index,
                embedding,
                predict_value,
            )

            if updated:
                total += 1
                surface_embeddings[surface_index] = embedding
            else:
                missing += 1

    print(f"Applied embeddings to {total} surfaces. Missing nodes: {missing}.")

    surface_feature_map = pyg_payload.get("face_feature_map", {})
    feature_vectors: Dict[int, List[List[float]]] = {}

    for surface_key, feature_idx in surface_feature_map.items():
        surface_key_str = str(surface_key)
        embedding = surface_embeddings.get(surface_key_str)
        if embedding is None:
            continue

        try:
            feature_idx_int = int(feature_idx)
        except (TypeError, ValueError):
            continue

        feature_vectors.setdefault(feature_idx_int, []).append(embedding)

    feature_updates = 0
    missing_features: List[int] = []
    with driver.session() as session:
        for feature_index, vectors in feature_vectors.items():
            try:
                averaged = _average_vectors(vectors)
            except ValueError as exc:
                print(f"Skip feature {feature_index}: {exc}")
                continue

            updated = session.execute_write(
                _write_feature_embedding,
                hash_value,
                feature_index,
                averaged,
                len(vectors),
            )

            if updated:
                feature_updates += 1
            else:
                missing_features.append(feature_index)

    print(f"Applied pooled embeddings to {feature_updates} machining features.")
    if missing_features:
        print(f"Missing machining features for indices: {sorted(missing_features)}")

    driver.close()

