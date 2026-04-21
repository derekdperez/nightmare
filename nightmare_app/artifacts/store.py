from __future__ import annotations

import hashlib
import json
import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterable

try:
    import zstandard as zstd
except Exception:  # pragma: no cover
    zstd = None

from shared.models import ArtifactMetadata
from shared.versioning import registry

_CHUNK_SIZE = 1024 * 1024


class ArtifactStore(ABC):
    @abstractmethod
    def put_bytes(self, *, artifact_type: str, root_domain: str, payload: bytes, media_type: str = "application/octet-stream", encoding: str = "binary", run_id: str = "", stage_id: str = "", worker_id: str = "") -> ArtifactMetadata:
        raise NotImplementedError

    @abstractmethod
    def get_bytes(self, storage_uri: str, *, compression: str = "identity") -> bytes:
        raise NotImplementedError

    @abstractmethod
    def exists(self, storage_uri: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def delete(self, storage_uri: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def stat(self, storage_uri: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def resolve_uri(self, storage_uri: str) -> Path:
        raise NotImplementedError

    def put_json(self, *, artifact_type: str, root_domain: str, payload: dict[str, Any], **kwargs: Any) -> ArtifactMetadata:
        return self.put_bytes(
            artifact_type=artifact_type,
            root_domain=root_domain,
            payload=json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            media_type="application/json",
            encoding="utf-8",
            **kwargs,
        )

    def put_ndjson(self, *, artifact_type: str, root_domain: str, rows: Iterable[dict[str, Any]], **kwargs: Any) -> ArtifactMetadata:
        encoded = b"".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
            for row in rows
        )
        return self.put_bytes(
            artifact_type=artifact_type,
            root_domain=root_domain,
            payload=encoded,
            media_type="application/x-ndjson",
            encoding="utf-8",
            **kwargs,
        )


class FileSystemArtifactStore(ArtifactStore):
    def __init__(self, root: str | os.PathLike[str], *, compression_threshold_bytes: int = 1_000_000, enable_compression: bool = True):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.objects_dir = self.root / "objects"
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self.compression_threshold_bytes = max(0, int(compression_threshold_bytes))
        self.enable_compression = bool(enable_compression)

    def _object_relpath(self, sha256_hex: str, *, compression: str) -> str:
        ext = ".zst" if compression == "zstd" else ".bin"
        return f"sha256/{sha256_hex[:2]}/{sha256_hex[2:4]}/{sha256_hex}{ext}"

    def _compress(self, payload: bytes) -> tuple[bytes, str]:
        if not self.enable_compression or len(payload) < self.compression_threshold_bytes:
            return payload, "identity"
        if zstd is None:
            return payload, "identity"
        compressor = zstd.ZstdCompressor(level=3)
        return compressor.compress(payload), "zstd"

    def _decompress(self, payload: bytes, compression: str) -> bytes:
        if compression == "identity":
            return payload
        if compression == "zstd":
            if zstd is None:
                raise RuntimeError("zstandard dependency is required to read zstd-compressed artifacts")
            return zstd.ZstdDecompressor().decompress(payload)
        raise ValueError(f"Unsupported compression: {compression}")

    def put_bytes(self, *, artifact_type: str, root_domain: str, payload: bytes, media_type: str = "application/octet-stream", encoding: str = "binary", run_id: str = "", stage_id: str = "", worker_id: str = "") -> ArtifactMetadata:
        raw = bytes(payload or b"")
        digest = hashlib.sha256(raw).hexdigest()
        stored, compression = self._compress(raw)
        relpath = self._object_relpath(digest, compression=compression)
        target = self.objects_dir / relpath
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            fd, tmp_name = tempfile.mkstemp(prefix="artifact-", suffix=".tmp", dir=str(target.parent))
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(stored)
                os.replace(tmp_name, target)
            finally:
                try:
                    if os.path.exists(tmp_name):
                        os.unlink(tmp_name)
                except Exception:
                    pass
        return ArtifactMetadata(
            artifact_id=f"{root_domain}:{artifact_type}:{digest[:16]}",
            artifact_type=str(artifact_type or "").strip().lower(),
            root_domain=str(root_domain or "").strip().lower(),
            sha256=digest,
            size_bytes=len(raw),
            media_type=str(media_type or "application/octet-stream"),
            encoding=str(encoding or "binary"),
            compression=compression,
            schema_version=registry.current_version("artifact_metadata"),
            storage_backend="filesystem",
            storage_uri=relpath,
            run_id=str(run_id or ""),
            stage_id=str(stage_id or ""),
            worker_id=str(worker_id or ""),
        )

    def get_bytes(self, storage_uri: str, *, compression: str = "identity") -> bytes:
        path = self.resolve_uri(storage_uri)
        payload = path.read_bytes()
        return self._decompress(payload, compression)

    def exists(self, storage_uri: str) -> bool:
        return self.resolve_uri(storage_uri).exists()

    def delete(self, storage_uri: str) -> bool:
        path = self.resolve_uri(storage_uri)
        try:
            path.unlink()
            return True
        except FileNotFoundError:
            return False

    def stat(self, storage_uri: str) -> dict[str, Any]:
        path = self.resolve_uri(storage_uri)
        details = path.stat()
        return {"path": str(path), "size_bytes": int(details.st_size), "mtime": float(details.st_mtime)}

    def resolve_uri(self, storage_uri: str) -> Path:
        rel = str(storage_uri or "").strip().lstrip("/").replace("..", "")
        return self.objects_dir / rel
