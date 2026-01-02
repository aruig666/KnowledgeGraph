import json
import pathlib
from utils.hash import  file_hash
from utils.neo4j import connect_neo4j

from entity.part import Part


def _normalize_identifier(value):
    if value is None:
        return None
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return None
        if trimmed.isdigit():
            try:
                return int(trimmed)
            except ValueError:
                return trimmed
        return trimmed
    if isinstance(value, (int, float)):
        return int(value)
    return value


def _ensure_part(tx, part: Part) -> None:
    tx.run(
        """
        MERGE (part_node:Part {PartId: $part_id})
        SET part_node.Path = $path,
            part_node.Title = $title,
            part_node.CreatedAt = $created_at,
            part_node.ContentHash = $content_hash
        """,
        part_id=part.part_id,
        path=part.path,
        title=part.title,
        created_at=part.created_at,
        content_hash=part.content_hash,
    )

def _link_process_operation(tx, part_id: str, process_index: int, operation_tag) -> None:
    tx.run(
        """
        MATCH (process:Process {PartId: $part_id, ProcessIndex: $process_index})
        MATCH (operation:__Operation__ {PartId: $part_id, Tag: $operation_tag})
        MERGE (process)-[:INCLUDES_OPERATION]->(operation)
        """,
        part_id=part_id,
        process_index=process_index,
        operation_tag=operation_tag,
    )


if __name__ == "__main__":

    # init = False
    init = False
    driver = connect_neo4j(init=init)
    # _ensure_unique_constraints(driver)

    hashfile=r"E:\dataset\cam\251225test\step\3DA2607A.stp"
    hash_value = file_hash(hashfile)


    json_file = pathlib.Path(r"E:\dataset\cam\251225test\mynet_mulit_kg\3DA2607A.json")
    para_dict = 0
    with open(json_file, 'r',encoding='utf-8') as file:
        para_dict = json.load(file)


    
    with driver.session() as session:
        part_metadata = Part.from_path(pathlib.Path(hashfile), content_hash=hash_value)
        session.execute_write(_ensure_part, part_metadata)

        processes = para_dict.get("processes", [])
        '''
        "processes": [
            {
                "index": 1,
                "operationTags": [
                    "77176",
                    "77836"
                ],
                "featureUnitList": [
                    7
                ],
                "volume": 0.0,
                "typeName": "TopFace",
                "featureDepth": 0.0
            },
        '''
        for process in processes:
            raw_process_index = process.get("index")
            try:
                process_index = int(raw_process_index)
            except (TypeError, ValueError):
                continue
            operation_tags = process.get("operationTags", []) or []
            for raw_tag in operation_tags:
                normalized_tag = _normalize_identifier(raw_tag)
                if normalized_tag is None:
                    continue
                session.execute_write(
                    _link_process_operation,
                    part_metadata.part_id,
                    process_index,
                    normalized_tag,
                )



    driver.close()

