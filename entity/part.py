
from dataclasses import dataclass
from datetime import datetime, timezone
import pathlib


@dataclass
class Part:
    part_id: str
    path: str
    title: str
    created_at: str
    content_hash: str

    @classmethod
    def from_path(cls, source_path: pathlib.Path, *, content_hash: str) -> "Part":
        resolved = source_path.resolve()
        title = resolved.stem or resolved.name

        try:
            stat_result = resolved.stat()
            created_at = datetime.fromtimestamp(stat_result.st_mtime, tz=timezone.utc).isoformat()
        except (FileNotFoundError, OSError):
            created_at = datetime.now(timezone.utc).isoformat()

        part_id = f"Part_{content_hash[:16]}" if content_hash else f"Part_{title}"
        return cls(
            part_id=part_id,
            path=str(resolved),
            title=title,
            created_at=created_at,
            content_hash=content_hash,
        )

    def get_id(self) -> str:
        return self.part_id