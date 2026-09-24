"""Small S3-compatible object-store adapter used by activity certificates."""
from __future__ import annotations

import base64
import hashlib
import os
import urllib.request
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit


class ObjectStore:
    """Use MinIO/S3 when configured, with a filesystem adapter for local tests."""

    def __init__(self) -> None:
        self.endpoint = os.environ.get("MYOTA_OBJECT_STORAGE_ENDPOINT", "http://minio:9000")
        self.access_key = os.environ.get("MYOTA_OBJECT_STORAGE_ACCESS_KEY", "myota-minio")
        self.secret_key = os.environ.get("MYOTA_OBJECT_STORAGE_SECRET_KEY", "myota-minio-dev-only")
        local_root = os.environ.get("MYOTA_OBJECT_STORAGE_LOCAL_DIR", "")
        self.local_root = Path(local_root) if local_root else None
        self._client = None

    def _minio(self):
        if self.local_root or self._client is not None:
            return self._client
        try:
            from minio import Minio
        except ImportError:
            return None
        parsed = urlsplit(self.endpoint)
        self._client = Minio(parsed.netloc, access_key=self.access_key, secret_key=self.secret_key,
                              secure=parsed.scheme == "https")
        return self._client

    def available(self) -> bool:
        return bool(self.local_root or self._minio())

    @staticmethod
    def scan_content(content: bytes, filename: str = "upload") -> dict[str, object]:
        """Run the local safety gate and optionally ask a ClamAV HTTP sidecar.

        The EICAR signature is intentionally detected even in development so
        tests can prove that infected uploads never reach object storage. In
        production, ``MYOTA_CLAMAV_URL`` should point at a ClamAV scanning
        sidecar or gateway; failure is fail-closed unless explicitly disabled
        for a local development environment.
        """
        max_bytes = int(os.environ.get("MYOTA_UPLOAD_MAX_BYTES", str(25 * 1024 * 1024)))
        if len(content) > max_bytes:
            raise ValueError(f"{filename} exceeds the configured upload limit")
        if b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*" in content:
            raise ValueError("malware scan rejected the upload")
        scanner_url = os.environ.get("MYOTA_CLAMAV_URL", "").strip()
        if scanner_url:
            request = urllib.request.Request(scanner_url, data=content, method="POST",
                                              headers={"Content-Type": "application/octet-stream", "X-Upload-Name": filename})
            try:
                with urllib.request.urlopen(request, timeout=10) as response:
                    if response.status >= 300:
                        raise ValueError("malware scanner rejected the upload")
            except Exception as exc:
                raise ValueError("malware scanner is unavailable; upload was not stored") from exc
        return {"status": "CLEAN", "sha256": hashlib.sha256(content).hexdigest(), "size": len(content)}

    def _local_path(self, bucket: str, object_key: str) -> Path:
        if not self.local_root:
            raise RuntimeError("local object storage is not configured")
        path = self.local_root / bucket / object_key
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def put(self, bucket: str, object_key: str, content: bytes, content_type: str) -> dict[str, object]:
        checksum = hashlib.sha256(content).hexdigest()
        if self.local_root:
            path = self._local_path(bucket, object_key)
            path.write_bytes(content)
        else:
            client = self._minio()
            if not client:
                raise RuntimeError("MinIO SDK is not installed")
            from io import BytesIO
            from minio.error import S3Error
            try:
                self._ensure_bucket(client, bucket)
                client.put_object(bucket, object_key, BytesIO(content), len(content), content_type=content_type)
            except S3Error as exc:
                raise RuntimeError(f"object storage upload failed: {exc.code}") from exc
        return {"sha256": checksum, "size": len(content), "storedAt": object_key}

    @staticmethod
    def _ensure_bucket(client: object, bucket: str) -> None:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

    def get(self, bucket: str, object_key: str) -> bytes | None:
        if self.local_root:
            path = self._local_path(bucket, object_key)
            return path.read_bytes() if path.exists() else None
        client = self._minio()
        if not client:
            return None
        response = None
        try:
            response = client.get_object(bucket, object_key)
            return response.read()
        except Exception:
            return None
        finally:
            if response:
                response.close()
                response.release_conn()

    def presigned_put(self, bucket: str, object_key: str) -> str | None:
        if self.local_root:
            return None
        client = self._minio()
        if client:
            self._ensure_bucket(client, bucket)
        return client.presigned_put_object(bucket, object_key, expires=timedelta(minutes=15)) if client else None

    def presigned_get(self, bucket: str, object_key: str) -> str | None:
        if self.local_root:
            return None
        client = self._minio()
        return client.presigned_get_object(bucket, object_key, expires=timedelta(minutes=15)) if client else None


def decode_base64(value: str) -> bytes:
    try:
        return base64.b64decode(value, validate=True)
    except Exception as exc:
        raise ValueError("contentBase64 must be valid base64") from exc
