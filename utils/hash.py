import hashlib
import json

def file_hash(path, algo="sha256", chunk_size=1024 * 1024):
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()[:16]

def generate_unique_id( prefix: str, **kwargs) -> str:
    obj_hash= generate_object_hash_id(kwargs)
    return f"{prefix}_{obj_hash}"


def generate_object_hash_id( **kwargs) -> str:
    payload = json.dumps(kwargs, sort_keys=True, ensure_ascii=True, default=str)
    digest = hashlib.sha256(payload.encode('utf-8')).hexdigest()
    return digest[:16]