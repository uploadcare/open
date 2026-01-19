from tempfile import NamedTemporaryFile
from typing import IO, Any

import boto3
import fitz
import urllib3
from botocore.exceptions import ClientError

from pdf2image import settings


MB = 2**20
CHUNK_SIZE = 2 * MB
STATE_SUFFIX = ".state.json"

CONTENT_TYPES = {"png": "image/png", "jpg": "image/jpeg"}

VALID_DPI_VALUES = {72, 96, 110, 150, 200, 250, 300, 600}
VALID_QUALITY_VALUES = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
VALID_OUTPUT_FORMATS = {"png", "jpg", "small.jpg"}

s3 = boto3.client(
    "s3",
    aws_access_key_id=settings.S3_READER["access_key"],
    aws_secret_access_key=settings.S3_READER["secret_key"],
)
http: urllib3.PoolManager = urllib3.PoolManager()


def state_file_key(output_prefix: str) -> str:
    return f"{output_prefix.rstrip('/')}{STATE_SUFFIX}"


def get_object(bucket: str, key: str) -> dict[str, Any] | None:
    try:
        return s3.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "NoSuchKey":
            return None
        raise


def _write_stream_to_temp(body, read_chunk) -> IO[bytes]:
    tmp = NamedTemporaryFile(delete=False)
    while True:
        chunk = read_chunk()
        if not chunk:
            break
        tmp.write(chunk)
    tmp.flush()
    return tmp


def download_input_file(bucket: str, key: str) -> IO[bytes] | None:
    obj = get_object(bucket, key)
    if obj is None:
        return None

    body = obj["Body"]
    return _write_stream_to_temp(body, lambda: body.read(CHUNK_SIZE))


def download_input_url(url: str) -> IO[bytes]:
    response = http.request("GET", url, preload_content=False)
    if response.status >= 400:
        response.release_conn()
        raise ValueError(f"Could not download input file, status {response.status}")

    tmp = _write_stream_to_temp(
        response, lambda: next(response.stream(CHUNK_SIZE), b"")
    )
    response.release_conn()
    return tmp


def build_output_key(output_prefix: str, filename: str) -> str:
    prefix = output_prefix.rstrip("/")
    return f"{prefix}/{filename}"


def _normalize_page(page: int | None, total_pages: int) -> list[int]:
    if page is not None:
        if page < 0:
            raise ValueError(f"Page must be >= 0, got {page}")
        page_number = min(page, total_pages - 1)
        return [page_number]
    return list(range(total_pages))


def convert_pdf(
    input_file: str,
    output_format: str = "png",
    dpi: int | None = None,
    page: int | None = None,
    quality: int | None = None,
) -> tuple[list[tuple[int, str]], str]:
    if output_format not in VALID_OUTPUT_FORMATS:
        raise ValueError(f"Unsupported output format: {output_format}")

    is_small = output_format == "small.jpg"
    fmt = "jpg" if is_small else output_format

    if is_small:
        dpi = 110
        quality = 50

    if dpi is None:
        dpi = settings.DEFAULT_DPI

    if dpi not in VALID_DPI_VALUES:
        raise ValueError(f"Unsupported dpi: {dpi}")

    if quality is None and fmt == "jpg":
        quality = settings.DEFAULT_JPG_QUALITY

    if quality is not None:
        if fmt != "jpg":
            raise ValueError("JPEG quality is supported only for jpg format")
        if quality not in VALID_QUALITY_VALUES:
            raise ValueError(f"Unsupported JPEG quality: {quality}")

    outputs: list[tuple[int, str]] = []
    save_kwargs: dict[str, Any] = {"output": fmt}
    if quality is not None:
        save_kwargs["jpg_quality"] = quality

    with fitz.open(input_file) as doc:
        if doc.page_count == 0:
            raise ValueError("PDF has no pages")

        pages = _normalize_page(page, doc.page_count)

        for page_number in pages:
            pix = doc[page_number].get_pixmap(dpi=dpi)
            tmp = NamedTemporaryFile(delete=False, suffix=f".{fmt}")
            pix.save(tmp.name, **save_kwargs)
            outputs.append((page_number, tmp.name))

    return outputs, fmt
