
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from utils.hash import file_hash
from utils.neo4j import connect_neo4j


def _collect_file_info(file_path: Path) -> Optional[Dict[str, object]]:
    try:
        stat_result = file_path.stat()
    except OSError:
        return None

    try:
        file_digest = file_hash(str(file_path))
    except OSError:
        return None

    modified_at = datetime.fromtimestamp(stat_result.st_mtime, tz=timezone.utc).isoformat()
    created_at = datetime.fromtimestamp(stat_result.st_ctime, tz=timezone.utc).isoformat()

    return {
        "hash": file_digest,
        "path": str(file_path.resolve()),
        "name": file_path.name,
        "stem": file_path.stem,
        "extension": file_path.suffix.lower(),
        "size": stat_result.st_size,
        "modified_at": modified_at,
        "created_at": created_at,
    }


def _ensure_file_group(tx, group_name: str) -> None:
    tx.run(
        """
        MERGE (group:FileGroup {Name: $group_name})
        """,
        group_name=group_name,
    )


def _upsert_file_variant(tx, group_name: str, file_info: Dict[str, object]) -> None:
    tx.run(
        """
        MATCH (group:FileGroup {Name: $group_name})
        MERGE (file:File {Hash: $hash})
        SET file.Path = $path,
            file.Name = $name,
            file.Stem = $stem,
            file.Extension = $extension,
            file.Size = $size,
            file.ModifiedAt = $modified_at,
            file.CreatedAt = $created_at
        MERGE (group)-[rel:HAS_FILE]->(file)
        SET rel.Extension = $extension
        """,
        group_name=group_name,
        hash=file_info["hash"],
        path=file_info["path"],
        name=file_info["name"],
        stem=file_info["stem"],
        extension=file_info["extension"],
        size=file_info["size"],
        modified_at=file_info["modified_at"],
        created_at=file_info["created_at"],
    )


if __name__ == "__main__":
    init = True
    driver = connect_neo4j(init=init)

    file_dir = Path(r"E:\dataset\cam\260108test\process_graph")
    file_stem = "cs1"

    candidate_suffixes = [".pdf", ".stp", ".prt"]
    file_infos = []

    for suffix in candidate_suffixes:
        candidate = file_dir / f"{file_stem}{suffix}"
        info = _collect_file_info(candidate)
        if info:
            file_infos.append(info)

    with driver.session() as session:
        session.execute_write(_ensure_file_group, file_stem)
        for info in file_infos:
            session.execute_write(_upsert_file_variant, file_stem, info)

    driver.close()