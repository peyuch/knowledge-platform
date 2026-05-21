"""MinIO object storage client wrapper."""

from __future__ import annotations

from typing import TYPE_CHECKING

from minio import Minio
from minio.error import S3Error

from core.config import settings

if TYPE_CHECKING:
    from io import BufferedIOBase


class MinioClient:
    """Thin wrapper around the MinIO SDK providing convenience methods."""

    def __init__(self) -> None:
        self._client = Minio(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )
        self._bucket = settings.minio_bucket

    # ------------------------------------------------------------------
    # bucket helpers
    # ------------------------------------------------------------------

    def ensure_bucket(self) -> None:
        """Create the configured bucket if it does not already exist."""
        if not self._client.bucket_exists(self._bucket):
            self._client.make_bucket(self._bucket)

    # ------------------------------------------------------------------
    # upload
    # ------------------------------------------------------------------

    def upload_bytes(self, object_name: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        """Upload raw *data* bytes and return the object name."""
        from io import BytesIO

        self._client.put_object(
            bucket_name=self._bucket,
            object_name=object_name,
            data=BytesIO(data),
            length=len(data),
            content_type=content_type,
        )
        return object_name

    def upload_file(self, object_name: str, file_path: str, content_type: str = "application/octet-stream") -> str:
        """Upload a local file at *file_path* and return the object name."""
        self._client.fput_object(
            bucket_name=self._bucket,
            object_name=object_name,
            file_path=file_path,
            content_type=content_type,
        )
        return object_name

    # ------------------------------------------------------------------
    # download
    # ------------------------------------------------------------------

    def download_bytes(self, object_name: str) -> bytes:
        """Download the object as raw bytes."""
        response = None
        try:
            response = self._client.get_object(self._bucket, object_name)
            return response.read()
        finally:
            if response is not None:
                response.close()
                response.release_conn()

    # ------------------------------------------------------------------
    # pre-signed URLs
    # ------------------------------------------------------------------

    def get_presigned_url(self, object_name: str, expires: int = 3600) -> str:
        """Return a pre-signed GET URL valid for *expires* seconds."""
        return self._client.presigned_get_object(self._bucket, object_name, expires=expires)

    # ------------------------------------------------------------------
    # metadata helpers
    # ------------------------------------------------------------------

    def object_exists(self, object_name: str) -> bool:
        """Return True if the object exists in the bucket."""
        try:
            self._client.stat_object(self._bucket, object_name)
            return True
        except S3Error:
            return False


# ------------------------------------------------------------------
# singleton
# ------------------------------------------------------------------

_minio_client: MinioClient | None = None


def get_minio() -> MinioClient:
    """Return the module-level MinioClient singleton, creating it on first call."""
    global _minio_client
    if _minio_client is None:
        _minio_client = MinioClient()
        _minio_client.ensure_bucket()
    return _minio_client
