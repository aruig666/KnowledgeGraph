import json
import pathlib
from secrets import token_hex
from typing import Dict

from entity.feature import MachiningFeature
from entity.part import Part
from entity.process import Process

from utils.hash import generate_unique_id, file_hash,generate_object_hash_id
from utils.neo4j import connect_neo4j



def _insert_face(tx, file_id: str, face_index: str, face_payload: dict) -> str:
    """Create or update a face node with a random hash identifier."""
    props = dict(face_payload)
    props["__fileId__"] = file_id
    props["__index__"] = face_index
    hash_value = generate_object_hash_id(**props)
    props["__hash__"] = hash_value
    identifier = f"Surface_{hash_value}"

    type_idx = props.get("face_type")
    type_name = props.get("face_type_name")
    tx.run(
        """
        MATCH (f:File {Hash: $file_id})
        MERGE (st:SurfaceType {name: $type_name, index: $type_idx})
        MERGE (s:Surface {__id__:$identifier, __fileId__:$file_id})
        SET s += $props
        MERGE (f)-[:HAS_SURFACE]->(s)
        MERGE (s)-[:OF_SURFACE_TYPE]->(st)
        """,
        file_id=file_id,
        type_idx=type_idx,
        type_name=type_name,
        part_id=file_id,
        identifier=identifier,
        props=props,
    )
    return identifier


def _insert_curve(tx, file_id: str, curve_key: str, curve_payload: dict) -> str:
    """Create or update a curve node with a random hash identifier."""
    props = dict(curve_payload)
    props["__fileId__"] = file_id
    props["__index__"] = curve_key
    # if "edge_idx" in props:
    #     del props["edge_idx"]

    hash_value = generate_object_hash_id(**props)
    props["__hash__"] = hash_value
    identifier= f"Curve_{hash_value}"
    type_idx= props.get("curve_type")
    type_name = props.get("curve_type_name")

    tx.run(
        """
        MATCH (f:File {Hash: $file_id})
        MERGE (ct:CurveType {name: $type_name, index: $type_idx})
        MERGE (c:Curve {__id__: $identifier, __fileId__: $file_id})
        SET c += $props
        MERGE (f)-[:HAS_CURVE]->(c)
        MERGE (c)-[:OF_CURVE_TYPE]->(ct)
        """,
        file_id=file_id,
        type_idx=type_idx,
        type_name=type_name,
        identifier=identifier,
        props=props,
    )
    return identifier


def _link_curve_surface(tx, file_id: str, curve_id: str, surface_id: str) -> None:
    tx.run(
        """
        MATCH (curve:Curve {__id__: $curve_id, __fileId__: $file_id})
        MATCH (surface:Surface {__id__: $surface_id, __fileId__: $file_id})
        MERGE (curve)-[:BOUNDARY_OF]->(surface)
        """,
        file_id=file_id,
        curve_id=curve_id,
        surface_id=surface_id,
    )


def _insert_machining_features(tx, file_id: str, feature, feature_index) -> str:

    props =feature
    # if 'faceTags' in props:
    #     del props['faceTags']
    if 'index' in props:
        del props['index']

    props['__fileId__']=file_id
    props['__index__']=feature_index
    hash_value = generate_object_hash_id(**props)
    props['__hash__']=hash_value
    identifier = f"MachiningFeature_{hash_value}"
    type_name = props.get("featureType")

    tx.run(
        """
        MERGE (mft:MachiningFeatureType {name: $type_name})
        MERGE (mf:MachiningFeature {__id__: $identifier, __fileId__: $file_id})
        SET mf += $props
        MERGE (mf)-[:OF_FEATURE_TYPE]->(mft)
        """,
        file_id=file_id,
        type_name=type_name,
        identifier=identifier,
        props=props,
    )
    return identifier


def _link_adjacent_features(tx, file_id: str, src_id: str, tar_id: str) -> None:
    tx.run(
        """
        MATCH (src_fe:MachiningFeature {__id__: $src_id, __fileId__: $file_id})
        MATCH (tar_fe:MachiningFeature {__id__: $tar_id, __fileId__: $file_id})
        MERGE (src_fe)-[:ADJACENT_MFEATURE]->(tar_fe)
        """,
        file_id=file_id,
        src_id=src_id,
        tar_id=tar_id,
    )


def _link_surface_feature(tx, file_id: str, surface_id: str, feature_id: str) -> None:
    tx.run(
        """
        MATCH (surface:Surface {__id__: $surface_id, __fileId__: $file_id})
        MATCH (feature:MachiningFeature {__id__: $feature_id, __fileId__: $file_id})
        MERGE (surface)-[:BELONGS_TO_FEATURE]->(feature)
        """,
        file_id=file_id,
        surface_id=surface_id,
        feature_id=feature_id,
    )

def _insert_process(tx, file_id: str, process) -> str:
    props=process
    type_name= props['typeName']
    p_indx=0
    if 'index' in props:
        p_indx= props['index']
        del props['index']
    props['__index__']=p_indx
    props['__fileId__']=file_id
    hash_value = generate_object_hash_id(**props)
    props['__hash__']=hash_value
    identifier = f"ProcessUnit_{hash_value}"

    tx.run(
        """
        MERGE (pt:ProcessUnitType {name: $type_name})
        MERGE (p:ProcessUnit {__id__: $identifier, __fileId__: $file_id})
        SET p += $props
        MERGE (p)-[:OF_PROCESS_UNIT_TYPE]->(pt)
        """,
        file_id=file_id,
        type_name=type_name,
        identifier=identifier,
        props=props,
    )
    return identifier,p_indx


def _link_process_adjacent(tx, file_id: str, left_id: str, right_id: str) -> None:
    if left_id == right_id:
        return

    tx.run(
        """
        MATCH (lhs:ProcessUnit {__id__: $left_id, __fileId__: $file_id})
        MATCH (rhs:ProcessUnit {__id__: $right_id, __fileId__: $file_id})
        MERGE (lhs)-[:ADJACENT_PROCESS]->(rhs)
        """,
        file_id=file_id,
        left_id=left_id,
        right_id=right_id,
    )


def _link_feature_process(tx, file_id: str, feature_id: str, process_id: str) -> None:
    tx.run(
        """
        MATCH (feature:MachiningFeature {__id__: $feature_id, __fileId__: $file_id})
        MATCH (process:ProcessUnit {__id__: $process_id, __fileId__: $file_id})
        MERGE (feature)-[:ASSIGNED_TO_PROCESS]->(process)
        """,
        file_id=file_id,
        feature_id=feature_id,
        process_id=process_id,
    )


def _link_process_operation(tx, file_id: str,prt_file_id: str, process_idx: str, operation_name) -> None:
    tx.run(
        """
        MATCH (process:ProcessUnit {__fileId__: $file_id, __id__: $process_idx})
        MATCH (operation:Operation {__fileId__: $prt_file_id, Name: $operation_name})
        MERGE (process)-[:INCLUDES_OPERATION]->(operation)
        """,
        file_id=file_id,
        prt_file_id=prt_file_id,
        process_idx=process_idx,
        operation_name=operation_name,
    )

if __name__ == "__main__":

    init = False
    # init = True
    driver = connect_neo4j(init=init)
    # _ensure_unique_constraints(driver)

    hashfile=r"E:\dataset\cam\260108test\process_graph\cs1.stp"
    hash_value = file_hash(hashfile)

    prtfile=r"E:\dataset\cam\260108test\process_graph\cs1.prt"
    prt_file_id = file_hash(prtfile)

    json_file = pathlib.Path(r"E:\dataset\cam\260108test\mynet_multi_kgv2\cs1.json")
    para_dict = 0
    with open(json_file, 'r',encoding='utf-8') as file:
        para_dict = json.load(file)

    with driver.session() as session:
        file_id=hash_value

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
            identifier = session.execute_write(_insert_face, file_id, face_key, face_payload)
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
            curve_id = session.execute_write(_insert_curve, file_id, curve_key, curve_payload)
            print(f"Upsert curve {curve_key} succeed with Id {curve_id}!")

            linked_surfaces = curve_payload.get('edge_idx', [])
            if isinstance(linked_surfaces, list):
                for surface_index in linked_surfaces:
                    surface_key = str(surface_index)
                    surface_id = surface_ids.get(surface_key)
                    if surface_id is None:
                        surface_id = surface_ids.get(surface_index)
                    if surface_id:
                        session.execute_write(_link_curve_surface, file_id, curve_id, surface_id)

        mf_idx_map={}
        machining_features=para_dict.get('features', [])
        '''
         "features": [
            {
                "index": 0,
                "faceTags": [ # drop face indices
                    27108
                ],
                "directionsCode": 3,
                "featureType": "OpenPocket"
            },
        
        '''
        for fe_id, feature in enumerate(machining_features):
            feature_identifier = session.execute_write(_insert_machining_features, file_id, feature,fe_id)
            mf_idx_map[fe_id]= feature_identifier


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
        for src_feature,tar_fe in machining_feature_edges:
            if src_feature == tar_fe:
                continue
            session.execute_write(_link_adjacent_features, file_id, mf_idx_map[src_feature], mf_idx_map[tar_fe])

        for surface_key, feature_idx in surface_feature_map.items():
            feature_id = mf_idx_map.get(feature_idx)
            surface_id = surface_ids.get(surface_key)
            if surface_id is None:
                surface_id = surface_ids.get(int(surface_key))
            if surface_id and feature_id:
                session.execute_write(_link_surface_feature, file_id, surface_id, feature_id)

        processes = para_dict.get('processes', [])
        process_index_map = {}
        process_operation_map = {}
        
        for process in processes:
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
            '''
            process_identifier,p_index = session.execute_write(_insert_process, file_id, process)
            process_index_map[p_index]= process_identifier
            process_operation_map[process_identifier]= process.get('operationNames', [])


        process_index = para_dict.get('process_index', [])
        '''
         "process_index": [
            [
                6,
                4
            ],
        '''
        for left_idx, right_idx in process_index:
            left_id = process_index_map.get(left_idx)
            right_id = process_index_map.get(right_idx)
            if left_id and right_id:
                if left_id == right_id:
                    continue
                session.execute_write(_link_process_adjacent, file_id, left_id, right_id)

        raw_feature_process_map = para_dict.get('feature_process', {})
        '''
        "feature_process": {
            "0": 4,
            "1": 6,
            "2": 5,
        },
        '''
        for fe_id, proc_idx in raw_feature_process_map.items():
            try:
                feature_index = int(fe_id)
                process_index_value = int(proc_idx)
            except (TypeError, ValueError):
                continue

            feature_id = mf_idx_map.get(feature_index)
            process_id = process_index_map.get(process_index_value)
            if feature_id and process_id:
                session.execute_write(_link_feature_process, file_id, feature_id, process_id)



        for process_id, operation_names in process_operation_map.items():
            for operation_name in operation_names:
                operation_name = operation_name.strip()
                if not operation_name:
                    continue
                session.execute_write(
                    _link_process_operation,
                    file_id,
                    prt_file_id,
                    process_id,
                    operation_name,
                )

    driver.close()


