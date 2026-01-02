import hashlib
import json
import pathlib
from typing import List

from utils.hash import generate_unique_id, file_hash
from utils.neo4j import connect_neo4j
from entity.part import Part


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


def _upsert_operation(tx, part_id: str, operation: dict) -> str:
    type_chain = _extract_type_chain(operation)

    labels = ['__Operation__'] + type_chain
    props = dict(operation)
    props['PartId'] = part_id
    tag_value = _normalize_identifier(props.get('Tag'))
    if tag_value is not None:
        props['Tag'] = tag_value
    identifier = generate_unique_id(prefix='Operation', **props)
    label_clause = ':'.join(labels)
    tx.run(
        f"""
        MERGE (o:{label_clause} {{Id: $identifier}})
        SET o += $props
        """,
        identifier=identifier,
        props=props,
    )
    return identifier


def _upsert_tool(tx, part_id: str, tool: dict) -> str:
    type_chain = _extract_type_chain(tool)

    labels: List[str] = []
    for label in ['__Tool__'] + [item for item in type_chain]:
        if label and label not in labels:
            labels.append(label)

    props = dict(tool)
    tag_value = _normalize_identifier(props.get('Tag'))
    if tag_value is None and not props.get('Name'):
        raise ValueError('Tool record needs Tag or Name for identification.')

    props['PartId'] = part_id
    identifier = generate_unique_id(prefix='Tool', **props)

    if tag_value is not None:
        props['Tag'] = tag_value
    elif 'Tag' in props:
        props['Tag'] = props.get('Tag')
    props['Id'] = identifier
    if type_chain:
        props['TypeChain'] = type_chain

    label_clause = ':'.join(labels)
    tx.run(
        f"""
        MERGE (t:{label_clause} {{Id: $identifier}})
        SET t += $props
        """,
        identifier=identifier,
        props=props,
    )
    return identifier


def _link_tool_usage_by_tag(tx, part_id: str, tool_tag, operation_tag) -> None:
    tx.run(
        """
        MATCH (tool:__Tool__ {PartId: $part_id, Tag: $tool_tag})
        MATCH (op:__Operation__ {PartId: $part_id, Tag: $operation_tag})
        MERGE (op)-[:USES_TOOL]->(tool)
        """,
        part_id=part_id,
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


def _upsert_feature_geometry(tx, part_id: str, feature: dict) -> str:
    type_chain = _extract_type_chain(feature)

    labels: List[str] = []
    for label in ['__Geometry__'] + [item for item in type_chain]:
        if label and label not in labels:
            labels.append(label)

    props = dict(feature)
    tag_value = _normalize_identifier(props.get('Tag'))
    if tag_value is None and not props.get('Name'):
        raise ValueError('FeatureGeometry record needs Tag or Name for identification.')

    props['PartId'] = part_id
    identifier = generate_unique_id(prefix='Geometry', **props)

    if tag_value is not None:
        props['Tag'] = tag_value
    elif 'Tag' in props:
        props['Tag'] = props.get('Tag')
    props['Id'] = identifier
    if type_chain:
        props['TypeChain'] = type_chain

    label_clause = ':'.join(labels)
    tx.run(
        f"""
        MERGE (f:{label_clause} {{Id: $identifier}})
        SET f += $props
        """,
        identifier=identifier,
        props=props,
    )
    return identifier


def _link_feature_orient_by_tag(tx, part_id: str, feature_tag, orient_tag) -> None:
    tx.run(
        """
        MATCH (feature:FeatureGeometry {PartId: $part_id, Tag: $feature_tag})
        MATCH (orient:OrientGeometry {PartId: $part_id, Tag: $orient_tag})
        MERGE (feature)-[:ALIGN_WITH]->(orient)
        """,
        part_id=part_id,
        feature_tag=feature_tag,
        orient_tag=orient_tag,
    )


def _upsert_orient_geometry(tx, part_id: str, orient: dict) -> str:
    type_chain = _extract_type_chain(orient)

    labels: List[str] = []
    for label in ['__Geometry__'] + [item for item in type_chain]:
        if label and label not in labels:
            labels.append(label)

    props = dict(orient)
    tag_value = _normalize_identifier(props.get('Tag'))
    if tag_value is None and not props.get('Name'):
        raise ValueError('OrientGeometry record needs Tag or Name for identification.')

    props['PartId'] = part_id
    identifier = generate_unique_id(prefix='Geometry', **props)

    if tag_value is not None:
        props['Tag'] = tag_value
    elif 'Tag' in props:
        props['Tag'] = props.get('Tag')
    props['Id'] = identifier
    if type_chain:
        props['TypeChain'] = type_chain

    label_clause = ':'.join(labels)
    tx.run(
        f"""
        MERGE (o:{label_clause} {{Id: $identifier}})
        SET o += $props
        """,
        identifier=identifier,
        props=props,
    )
    return identifier


def _link_orient_operation_by_tag(tx, part_id: str, orient_tag, operation_tag) -> None:
    tx.run(
        """
        MATCH (orient:OrientGeometry {PartId: $part_id, Tag: $orient_tag})
        MATCH (operation:__Operation__ {PartId: $part_id, Tag: $operation_tag})
        MERGE (orient)-[:ORIENTS]->(operation)
        """,
        part_id=part_id,
        orient_tag=orient_tag,
        operation_tag=operation_tag,
    )


def _link_first_operation(tx, part_id: str, operation_id: str) -> None:
    tx.run(
        """
        MATCH (part_node:Part {PartId: $part_id})
        MATCH (operation:__Operation__ {Id: $operation_id, PartId: $part_id})
        MERGE (part_node)-[:FIRST_STEP]->(operation)
        """,
        part_id=part_id,
        operation_id=operation_id,
    )


def _link_last_operation(tx, part_id: str, operation_id: str) -> None:
    tx.run(
        """
        MATCH (part_node:Part {PartId: $part_id})
        MATCH (operation:__Operation__ {Id: $operation_id, PartId: $part_id})
        MERGE (part_node)-[:LAST_STEP]->(operation)
        """,
        part_id=part_id,
        operation_id=operation_id,
    )


def _link_next_operation(tx, part_id: str, current_id: str, next_id: str) -> None:
    if current_id == next_id:
        return

    tx.run(
        """
        MATCH (current:__Operation__ {Id: $current_id, PartId: $part_id})
        MATCH (next:__Operation__ {Id: $next_id, PartId: $part_id})
        MERGE (current)-[:NEXT_STEP]->(next)
        """,
        part_id=part_id,
        current_id=current_id,
        next_id=next_id,
    )


if __name__ == "__main__":

    # init = False
    init = False
    driver = connect_neo4j(init=init)
    # _ensure_unique_constraints(driver)
    hashfile = pathlib.Path(r"E:\dataset\cam\251225test\step\3DA2607A.stp")
    hash_value = file_hash(str(hashfile))
    print(f"File hash for {hashfile}: {hash_value}")

    json_file = pathlib.Path(r"E:\dataset\cam\251225test\kGraph_json\3DA2607A.json")
    para_dict = 0
    with open(json_file, 'r',encoding='utf-8') as file:
        para_dict = json.load(file)

    operations = para_dict['operations']

    with driver.session() as session:
        part = Part.from_path(hashfile, content_hash=hash_value)


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
            identifier = session.execute_write(_upsert_operation, part.part_id, operation)
            operation_records.append(identifier)

            print(f"Upsert operation {operation.get('Name', operation.get('Tag'))} succeed!")

        if operation_records:
            session.execute_write(_link_first_operation, part.part_id, operation_records[0])
            session.execute_write(_link_last_operation, part.part_id, operation_records[-1])

            for idx in range(len(operation_records) - 1):
                current_id = operation_records[idx]
                next_id = operation_records[idx + 1]
                session.execute_write(_link_next_operation, part.part_id, current_id, next_id)


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
            session.execute_write(_upsert_tool, part.part_id, tool)

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
                session.execute_write(_link_tool_usage_by_tag, part.part_id, tool_tag, op_tag)
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
            session.execute_write(_upsert_feature_geometry, part.part_id, feature)

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
            session.execute_write(_upsert_orient_geometry, part.part_id, orient)

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
                session.execute_write(_link_feature_orient_by_tag, part.part_id, feature_tag, orient_tag)
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
                session.execute_write(_link_orient_operation_by_tag, part.part_id, orient_tag, operation_tag)
                linked_count += 1

            if linked_count:
                print(f"Linked orient {orient_tag} to {linked_count} operations")

        nCGroups = para_dict.get('nCGroups', {})


    driver.close()