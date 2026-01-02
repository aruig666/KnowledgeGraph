import json
import pathlib
from secrets import token_hex
from typing import Dict

from entity.feature import MachiningFeature
from entity.part import Part
from entity.process import Process

from utils.hash import generate_unique_id, file_hash
from utils.neo4j import connect_neo4j



def _insert_face(tx, part_id: str, face_key: str, face_payload: dict) -> str:
    """Create or update a face node with a random hash identifier."""
    props = dict(face_payload)
    props["FaceKey"] = face_key
    props["PartId"] = part_id
    hash_value = generate_unique_id( prefix="Surface", **props)
    props["HashIndex"] = hash_value
    identifier = f"Surface_{hash_value}"


    tx.run(
        """
        MERGE (part_node:Part {PartId: $part_id})
        MERGE (surface:Surface {Id: $identifier})
        SET surface += $props
        MERGE (part_node)-[:HAS_SURFACE]->(surface)
        """,
        part_id=part_id,
        identifier=identifier,
        props=props,
    )
    return identifier


def _insert_curve(tx, part_id: str, curve_key: str, curve_payload: dict) -> str:
    """Create or update a curve node with a random hash identifier."""
    props = dict(curve_payload)
    props["CurveKey"] = curve_key
    props["PartId"] = part_id
    hash_value = generate_unique_id(prefix="Curve", **props)
    props["HashIndex"] = hash_value
    identifier = f"Curve_{hash_value}"

    tx.run(
        """
        MATCH (part_node:Part {PartId: $part_id})
        MERGE (curve:Curve {Id: $identifier})
        SET curve += $props
        """,
        part_id=part_id,
        identifier=identifier,
        props=props,
    )
    return identifier


def _link_curve_surface(tx, part_id: str, curve_id: str, surface_id: str) -> None:
    tx.run(
        """
        MATCH (curve:Curve {Id: $curve_id, PartId: $part_id})
        MATCH (surface:Surface {Id: $surface_id, PartId: $part_id})
        MERGE (curve)-[:BOUNDARY_OF]->(surface)
        """,
        part_id=part_id,
        curve_id=curve_id,
        surface_id=surface_id,
    )


def _insert_feature(tx, part_id: str, feature: MachiningFeature) -> str:

    props = {
        "FeatureIndex": feature.index,
        "PartId": part_id,
    }
    hash_value = generate_unique_id(prefix="MachiningFeature", **props)
    props["HashIndex"] = hash_value
    identifier = f"MachiningFeature_{hash_value}"
    if feature.surfaces:
        props["SurfaceKeys"] = sorted(feature.surfaces)
    if feature.neighbors:
        props["NeighborIndices"] = sorted(feature.neighbors)

    tx.run(
        """
        MATCH (part_node:Part {PartId: $part_id})
        MERGE (feature:MachiningFeature {Id: $identifier})
        SET feature += $props
        """,
        part_id=part_id,
        identifier=identifier,
        props=props,
    )
    return identifier


def _link_surface_feature(tx, part_id: str, surface_id: str, feature_id: str) -> None:
    tx.run(
        """
        MATCH (surface:Surface {Id: $surface_id, PartId: $part_id})
        MATCH (feature:MachiningFeature {Id: $feature_id, PartId: $part_id})
        MERGE (surface)-[:BELONGS_TO_FEATURE]->(feature)
        """,
        part_id=part_id,
        surface_id=surface_id,
        feature_id=feature_id,
    )


def _link_adjacent_features(tx, part_id: str, left_id: str, right_id: str) -> None:
    if left_id == right_id:
        return

    tx.run(
        """
        MATCH (lhs:MachiningFeature {Id: $left_id, PartId: $part_id})
        MATCH (rhs:MachiningFeature {Id: $right_id, PartId: $part_id})
        MERGE (lhs)-[:ADJACENT_TO]->(rhs)
        """,
        part_id=part_id,
        left_id=left_id,
        right_id=right_id,
    )


def _insert_process(tx, part_id: str, process: Process) -> str:
    props = dict(process.payload) if isinstance(process.payload, dict) else {}
    props["ProcessIndex"] = process.index
    props["PartId"] = part_id
    hash_value = generate_unique_id(prefix="Process", **props)
    props["HashIndex"] = hash_value
    identifier = f"Process_{hash_value}"

    if process.features:
        props["FeatureIndices"] = sorted(process.features)
    if process.neighbors:
        props["NeighborIndices"] = sorted(process.neighbors)

    tx.run(
        """
        MATCH (part_node:Part {PartId: $part_id})
        MERGE (process:Process {Id: $identifier})
        SET process += $props
        """,
        part_id=part_id,
        identifier=identifier,
        props=props,
    )
    return identifier


def _link_process_adjacent(tx, part_id: str, left_id: str, right_id: str) -> None:
    if left_id == right_id:
        return

    tx.run(
        """
        MATCH (lhs:Process {Id: $left_id, PartId: $part_id})
        MATCH (rhs:Process {Id: $right_id, PartId: $part_id})
        MERGE (lhs)-[:ADJACENT_PROCESS]->(rhs)
        """,
        part_id=part_id,
        left_id=left_id,
        right_id=right_id,
    )


def _link_feature_process(tx, part_id: str, feature_id: str, process_id: str) -> None:
    tx.run(
        """
        MATCH (feature:MachiningFeature {Id: $feature_id, PartId: $part_id})
        MATCH (process:Process {Id: $process_id, PartId: $part_id})
        MERGE (feature)-[:ASSIGNED_TO_PROCESS]->(process)
        """,
        part_id=part_id,
        feature_id=feature_id,
        process_id=process_id,
    )

def _insert_part(tx, part: Part) -> str:
    props = {
        "PartId": part.part_id,
        "Path": part.path,
        "Title": part.title,
        "CreatedAt": part.created_at,
        "ContentHash": part.content_hash,
    }

    tx.run(
        """
        MERGE (part_node:Part {PartId: $part_id})
        SET part_node += $props
        """,
        part_id=part.part_id,
        props=props,
    )
    return part.part_id


if __name__ == "__main__":

    # init = False
    init = True
    driver = connect_neo4j(init=init)
    # _ensure_unique_constraints(driver)

    hashfile=r"E:\dataset\cam\251225test\step\3DA2607A.stp"
    hash_value = file_hash(hashfile)


    json_file = pathlib.Path(r"E:\dataset\cam\251225test\mynet_mulit_kg\3DA2607A.json")
    para_dict = 0
    with open(json_file, 'r',encoding='utf-8') as file:
        para_dict = json.load(file)


    
    with driver.session() as session:
        part = Part.from_path(pathlib.Path(hashfile), content_hash=hash_value)
        session.execute_write(_insert_part, part)

        surfaces = para_dict['face_dict']
        surface_ids = {}

        for face_key, face_payload in surfaces.items():
            '''
              "face_dict": {
                "0": {
                    "face_vector": [
                        -0.0,
                        -0.0,
                        -1.0
                    ],
                    "face_type": "plane",
                    "face_dimless": "CONVEX",
                    "area": 4789.049,
                    "closed_u": false,
                    "closed_v": false,
                    "feature_type": 0
                },
            '''
            identifier = session.execute_write(_insert_face, part.part_id, face_key, face_payload)
            print(f"Upsert face {face_key} succeed with Id {identifier}!")
            surface_ids[face_key] = identifier

        curves = para_dict.get('edge_dict', {})
        for curve_key, curve_payload in curves.items():
            '''
            "edge_dict": {
                "0": {
                    "edge_idx": [
                        0,
                        7
                    ],
                    "edge_vector": [
                        -1.0,
                        0.0,
                        0.0
                    ],
                    "edge_dimless": "CONVEX",
                    "length": 60.0,
                    "closed": false,
                    "edge_type": "line"
                },
            '''
            curve_id = session.execute_write(_insert_curve, part.part_id, curve_key, curve_payload)
            print(f"Upsert curve {curve_key} succeed with Id {curve_id}!")

            linked_surfaces = curve_payload.get('edge_idx', [])
            if isinstance(linked_surfaces, list):
                for surface_index in linked_surfaces:
                    surface_key = str(surface_index)
                    surface_id = surface_ids.get(surface_key)
                    if surface_id is None:
                        surface_id = surface_ids.get(surface_index)
                    if surface_id:
                        session.execute_write(_link_curve_surface, part.part_id, curve_id, surface_id)


        machining_feature_edges = para_dict.get('feature_index', [])
        surface_feature_map = para_dict.get('face_feature_map', {})
        '''
        "feature_index": [
    [
        1,
        10
    ],
         "face_feature_map": {
    "0": 1,
    "1": 12,
    "2": 2,
        '''

        feature_graph = MachiningFeature.build_graph(surface_feature_map, machining_feature_edges)
        feature_ids: Dict[int, str] = {}

        for feature in feature_graph.values():
            feature_identifier = session.execute_write(_insert_feature, part.part_id, feature)
            feature_ids[feature.index] = feature_identifier
            for surface_key in feature.surfaces:
                surface_id = surface_ids.get(surface_key)
                if surface_id is None:
                    surface_id = surface_ids.get(str(surface_key))
                if surface_id:
                    session.execute_write(_link_surface_feature, part.part_id, surface_id, feature_identifier)

        for feature in feature_graph.values():
            left_id = feature_ids.get(feature.index)
            if not left_id:
                continue
            for neighbor_index in feature.neighbors:
                if feature.index >= neighbor_index:
                    continue
                right_id = feature_ids.get(neighbor_index)
                if right_id:
                    session.execute_write(_link_adjacent_features, part.part_id, left_id, right_id)

        processes = para_dict.get('processes', [])
        process_index = para_dict.get('process_index', [])
        raw_feature_process_map = para_dict.get('feature_process', {})
        if isinstance(raw_feature_process_map, dict):
            feature_process_map = raw_feature_process_map
        else:
            feature_process_map = {}

        '''
        "processes": [
        {
            "index": 1,
            "operationNames": [
                "F1_ROUGHTOPFACE_P1",
                "F1_FINISHTOPFACE_P2"
            ],
            "featureUnitList": [
                7
            ],
            "volume": 0.0,
            "typeName": "TopFace",
            "featureDepth": 0.0
        },

         "process_index": [
        [
            6,
            4
        ],

        
        "feature_process": {
                "0": 4,
                "1": 6,
                "2": 5,
        '''

        process_graph = Process.build_graph(processes, process_index, feature_process_map)
        process_ids: Dict[int, str] = {}

        for process_node in process_graph.values():
            process_identifier = session.execute_write(_insert_process, part.part_id, process_node)
            process_ids[process_node.index] = process_identifier

        for process_node in process_graph.values():
            left_id = process_ids.get(process_node.index)
            if not left_id:
                continue
            for neighbor_index in process_node.neighbors:
                if process_node.index >= neighbor_index:
                    continue
                right_id = process_ids.get(neighbor_index)
                if right_id:
                    session.execute_write(_link_process_adjacent, part.part_id, left_id, right_id)

        for feature_key, raw_process_idx in feature_process_map.items():
            try:
                feature_index = int(feature_key)
                process_index_value = int(raw_process_idx)
            except (TypeError, ValueError):
                continue

            feature_id = feature_ids.get(feature_index)
            process_id = process_ids.get(process_index_value)
            if feature_id and process_id:
                session.execute_write(_link_feature_process, part.part_id, feature_id, process_id)

    driver.close()


