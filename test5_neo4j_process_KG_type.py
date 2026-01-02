import hashlib
import json
import pathlib
from typing import List

from utils.hash import generate_unique_id, file_hash,generate_object_hash_id
from utils.neo4j import connect_neo4j
from entity.part import Part
from entity.operation_type import OperationType


def _normalize_identifier(value):
    if value is None:
        return None
    if isinstance(value, str):
        trimmed = value.strip()
        if trimmed.isdigit():
            return int(trimmed)
        return trimmed
    return value



def _normalize_for_hash(value):
    if isinstance(value, dict):
        return {str(k): _normalize_for_hash(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_for_hash(item) for item in value]
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)


def _upsert_operation(tx, file_id: str, operation: dict) -> str:

    type_name=operation.get('Type')
    props = dict(operation)
    props['__fileId__'] = file_id
    obj_hash = generate_object_hash_id(**props)
    identifier="Operation_"+obj_hash
    props['__hash__']=obj_hash
    tx.run(
        f"""
        MERGE (t:OperationType {{name: $type_name}})
        MERGE ( op:Operation {{
            __id__: $identifier,
            __fileId__: $file_id
        }}
        )
        SET op += $props
        MERGE (op)-[:OF_TYPE]->(t)
        """,
        file_id=file_id,
        type_name=type_name,
        identifier=identifier,
        props=props,
    )
    return identifier


def _upsert_tool(tx, file_id: str, tool: dict) -> str:
    type_name=tool.get("Type")
    sub_type_name=tool.get("SubType")
    props = dict(tool)
    props['__fileId__'] = file_id
    obj_hash = generate_object_hash_id(**props)
    identifier="Tool_"+obj_hash
    props['__hash__']=obj_hash

    tx.run(
        f"""
        MERGE (t:ToolType {{name: $type_name}})
        MERGE (ts:SubToolType {{name: $sub_type_name}})
        MERGE (tool:Tool {{
            __id__: $identifier,
            __fileId__: $file_id
        }}
        )
        MERGE (ts)-[:OF_TYPE]->(t)
        MERGE (tool)-[:OF_TYPE]->(ts)
        SET tool += $props
        """,
        file_id=file_id,
        type_name=type_name,
        sub_type_name=sub_type_name,
        identifier=identifier,
        props=props,
    )
    return identifier


def _link_tool_usage_by_tag(tx, file_id: str, tool_tag, operation_tag) -> None:
    tx.run(
        """
        MATCH (tool:Tool {__fileId__: $file_id, Tag: $tool_tag})
        MATCH (op:Operation {__fileId__: $file_id, Tag: $operation_tag})
        MERGE (op)-[:USES_TOOL]->(tool)
        """,
        file_id=file_id,
        tool_tag=tool_tag,
        operation_tag=operation_tag,
    )



def _extract_type_chain(obj: dict) -> List[str]:
    result: List[str] = []
    sub_type = obj.get('SubType')
    if sub_type:
        result.append(str("__" + obj.get('Type') + "__"))
        result.append(str(sub_type))
    else:
        result.append(str(obj.get('Type')))
    return result


def _upsert_feature_geometry(tx, file_id: str, feature: dict) -> str:
    type_name=feature.get("Type")
    props = dict(feature)
    props['__fileId__'] = file_id
    obj_hash=generate_object_hash_id(**props)
    identifier="Geometry_"+obj_hash
    props['__hash__']=obj_hash
    tx.run(
        f"""
        MERGE (gt:GeometryType {{name: $type_name}})
        MERGE (g:Geometry {{
            __id__: $identifier,
            __fileId__: $file_id
            }}
        )
        MERGE (g)-[:OF_TYPE]->(gt)
        SET g += $props
        """,
        type_name=type_name,
        file_id=file_id,
        identifier=identifier,
        props=props,
    )
    return identifier


def _link_feature_orient_by_tag(tx, file_id: str, feature_tag, orient_tag) -> None:
    tx.run(
        """
        MATCH (fg:Geometry {__fileId__: $file_id, Tag: $feature_tag})
        MATCH (orient:Geometry {__fileId__: $file_id, Tag: $orient_tag})
        MERGE (fg)-[:HAS_SETUP]->(orient)
        """,
        file_id=file_id,
        feature_tag=feature_tag,
        orient_tag=orient_tag,
    )


def _upsert_orient_geometry(tx, part_id: str, orient: dict) -> str:
    type_name=orient.get("Type")
    props = dict(orient)
    props['__fileId__'] = file_id
    obj_hash=generate_object_hash_id(**props)
    identifier="Geometry_"+obj_hash
    props['__hash__']=obj_hash

    tx.run(
        f"""
        MERGE (gt:GeometryType {{name: $type_name}})
        MERGE (o:Geometry {{__id__: $identifier, __fileId__: $file_id}})
        MERGE (o)-[:OF_TYPE]->(gt)
        SET o += $props
        """,
        type_name=type_name,
        file_id=part_id,
        identifier=identifier,
        props=props,
    )
    return identifier


def _link_orient_operation_by_tag(tx, file_id: str, orient_tag, operation_tag) -> None:
    tx.run(
        """
        MATCH (orient:Geometry {__fileId__: $file_id, Tag: $orient_tag})
        MATCH (operation:Operation {__fileId__: $file_id, Tag: $operation_tag})
        MERGE (orient)-[:HAS_OPERATION]->(operation)
        """,
        file_id=file_id,
        orient_tag=orient_tag,
        operation_tag=operation_tag,
    )


def _link_first_operation(tx, file_id: str, operation_id: str) -> None:
    '''
    # MERGE (part)-[:HAS_OPERATION]->(op)
    '''
    tx.run(
        """
        MATCH (file:File {Hash: $file_id})
        MATCH (operation:Operation {__id__: $operation_id, __fileId__: $file_id})
        MERGE (file)-[:FIRST_OPERATION]->(operation)
        """,
        file_id=file_id,
        operation_id=operation_id,
    )


def _link_last_operation(tx, file_id: str, operation_id: str) -> None:
    tx.run(
        """
        MATCH (part_node:Part {Hash: $file_id})
        MATCH (operation:Operation {__id__: $operation_id, __fileId__: $file_id})
        MERGE (part_node)-[:LAST_OPERATION]->(operation)
        """,
        file_id=file_id,
        operation_id=operation_id,
    )


def _link_next_operation(tx, file_id: str, current_id: str, next_id: str) -> None:
    if current_id == next_id:
        return

    tx.run(
        """
        MATCH (current:Operation {__id__: $current_id, __fileId__: $file_id})
        MATCH (next:Operation {__id__: $next_id, __fileId__: $file_id})
        MERGE (current)-[:NEXT_STEP]->(next)
        """,
        file_id=file_id,
        current_id=current_id,
        next_id=next_id,
    )


if __name__ == "__main__":

    # init = False
    init = False
    driver = connect_neo4j(init=init)
    # _ensure_unique_constraints(driver)
    hashfile = pathlib.Path(r"E:\dataset\cam\251225test\process_graph\3DA2607A.prt")
    hash_value = file_hash(str(hashfile))
    print(f"File hash for {hashfile}: {hash_value}")

    json_file = pathlib.Path(r"E:\dataset\cam\251225test\prt_kg_json\3DA2607A.json")
    para_dict = 0
    with open(json_file, 'r',encoding='utf-8') as file:
        para_dict = json.load(file)

    operations = para_dict['operations']

    with driver.session() as session:
        # part = Part.from_path(hashfile, content_hash=hash_value)
        file_id=hash_value

        operation_records = []

        for operation in operations:
            '''
            {

                "Tag": 26897,
                "Name": "F1_FINISHTOPFACE_P2",
                "Type": "VolumeBased25DMillingOperation",
                "Number": 10,
                "AdjustRegister": 10,
                "SurfaceSpeed": 0.0,
            },
            {

                "Tag": 26942,
                "Name": "F2_ROUGH_P1",
                "Type": "CavityMillingBuilder",
                "Number": 11,
                "AdjustRegister": 11,
                "SurfaceSpeed": 0.0,

            }
            '''
            identifier = session.execute_write(_upsert_operation, file_id, operation)
            operation_records.append(identifier)

            print(f"Upsert operation {operation.get('Name', operation.get('Tag'))} succeed!")

        if operation_records:
            session.execute_write(_link_first_operation, file_id, operation_records[0])
            session.execute_write(_link_last_operation, file_id, operation_records[-1])

            for idx in range(len(operation_records) - 1):
                current_id = operation_records[idx]
                next_id = operation_records[idx + 1]
                session.execute_write(_link_next_operation, file_id, current_id, next_id)


        tools = para_dict.get('tools', [])
        for tool in tools:
            '''
            {
                "Tag": 26919,
                "Name": "X-2-R0-B3",
                "Type": "Mill",
                "SubType": "Mill5",
                "Description": "2mm粗铣刀"
            },
            {
                "Tag": 26920,
                "Name": "X-3-R0-B3",
                "Type": "Mill",
                "SubType": "Mill5",
                "Description": "3mm粗铣刀",
                "Material": "TMC0_00001"
            }
            '''
            session.execute_write(_upsert_tool, file_id, tool)

            print(f"Upsert tool {tool.get('Name', tool.get('Tag'))} succeed!")

        tool_operation_map = para_dict.get('toolDict', {})
        '''
        "toolDict": {
            "26919": [
                26942,
                26943
            ],
        '''
        for tool_key, operation_indices in tool_operation_map.items():
            tool_tag = _normalize_identifier(tool_key)
            if tool_tag is None:
                continue
            if not isinstance(operation_indices, (list, tuple, set)):
                operation_iterable = [operation_indices]
            else:
                operation_iterable = operation_indices

            linked_count = 0
            for raw_op_tag in operation_iterable:
                op_tag = _normalize_identifier(raw_op_tag)
                if op_tag is None:
                    continue
                session.execute_write(_link_tool_usage_by_tag, file_id, tool_tag, op_tag)
                linked_count += 1

            if linked_count:
                print(f"Linked tool {tool_tag} to {linked_count} operations")

        feature_geometries = para_dict.get('featureGeometrys', [])
        '''
        "featureGeometrys": [
            {
                "Tag": 26916,
                "Name": "MY_WORKPIECE",
                "Type": "FeatureGeometry",
                "BlankDefinitionType": "FromGeometry",
                "BlockOffsetNegativeX": 0.0,
                "BlockOffsetNegativeY": 0.0,
                "BlockOffsetNegativeZ": 0.0,
                "BlockOffsetPositiveX": 0.0,
                "BlockOffsetPositiveY": 0.0,
                "BlockOffsetPositiveZ": 0.0
            }
        ]
        '''
        for feature in feature_geometries:
            session.execute_write(_upsert_feature_geometry, file_id, feature)

            print(f"Upsert feature geometry {feature.get('Name', feature.get('Tag'))} succeed!")

        orient_geometries = para_dict.get('orientGeometries', [])
        '''
        "orientGeometries": [
            {
                "Tag": 27020,
                "Name": "MCS",
                "Type": "OrientGeometry",
                "location": [47.5, 0.0, -74.62]
            }
        ]
        '''
        for orient in orient_geometries:
            session.execute_write(_upsert_orient_geometry, file_id, orient)

            print(f"Upsert orient geometry {orient.get('Name', orient.get('Tag'))} succeed!")

        feature_geometry_map = para_dict.get('featureGeometryDict', {})
        '''
        "featureGeometryDict": {
            "26916": [27020, 27543, 27024, 27574]
        }
        '''
        for feature_tag_key, orient_list in feature_geometry_map.items():
            feature_tag = _normalize_identifier(feature_tag_key)
            if feature_tag is None:
                continue

            if not isinstance(orient_list, (list, tuple, set)):
                orient_iterable = [orient_list]
            else:
                orient_iterable = orient_list

            linked_count = 0
            for raw_orient_tag in orient_iterable:
                orient_tag = _normalize_identifier(raw_orient_tag)
                if orient_tag is None:
                    continue
                session.execute_write(_link_feature_orient_by_tag, file_id, feature_tag, orient_tag)
                linked_count += 1

            if linked_count:
                print(f"Linked feature {feature_tag} to {linked_count} orient geometries")

        orient_operation_map = para_dict.get('orientGeometryDict', {})
        '''
        "orientGeometryDict": {
            "27020": [
                26896,
                26897,
                26942,
                26949,
                26948,
                26950,
                26944,
                26946
            ],
        '''
        for orient_tag_key, operations_list in orient_operation_map.items():
            orient_tag = _normalize_identifier(orient_tag_key)
            if orient_tag is None:
                continue

            if not isinstance(operations_list, (list, tuple, set)):
                operations_iterable = [operations_list]
            else:
                operations_iterable = operations_list

            linked_count = 0
            for raw_operation_tag in operations_iterable:
                operation_tag = _normalize_identifier(raw_operation_tag)
                if operation_tag is None:
                    continue
                session.execute_write(_link_orient_operation_by_tag, file_id, orient_tag, operation_tag)
                linked_count += 1

            if linked_count:
                print(f"Linked orient {orient_tag} to {linked_count} operations")

        nCGroups = para_dict.get('nCGroups', {})


    driver.close()