"""
PDF to image conversion Lambda.

State is stored in S3 next to the output prefix as `<output_prefix>.state.json`.
The flow mirrors gif2video: reuse previous state when possible, respect timeouts,
and avoid reprocessing finished jobs.
"""

import os
import sys
import time
import zipfile
from datetime import datetime
from json import dumps, loads
from os import makedirs
from tempfile import NamedTemporaryFile
from typing import Any, Literal, TypedDict

import rollbar

from pdf2image import settings
from pdf2image.utils import (
    CONTENT_TYPES, build_output_key, convert_pdf, download_input_url, get_object, s3,
    state_file_key)


makedirs(settings.FILE_CACHE_DIR, exist_ok=True)


Status = Literal["pending", "success", "fail", None]


class OutputEntry(TypedDict):
    page: int
    key: str


class TargetFile(TypedDict, total=False):
    id: int
    name: str
    size: int


State = dict[str, Any]
Event = dict[str, Any]


def _save_state(bucket: str, key: str, state: State) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=dumps(state))


def _load_state(bucket: str, key: str) -> State | None:
    obj = get_object(bucket, key)
    if not obj:
        return None
    return loads(obj["Body"].read().decode())


def _first_output_exists(state: State) -> bool:
    outputs = state.get("outputs") or []
    if not outputs:
        return False

    key = outputs[0].get("key")
    bucket = state.get("bucket")
    return bool(key and bucket and get_object(bucket, key))


def _has_timed_out(prev_state: State, now: datetime) -> bool:
    started_ts = prev_state.get("datetime_started")
    if started_ts is None:
        return False

    started = datetime.fromtimestamp(started_ts)
    return (now - started).total_seconds() >= settings.TIMEOUT


def _create_initial_state(
    *,
    start: datetime,
    output_bucket: str,
    output_prefix: str,
    output_format: str,
    dpi: int,
    page: int | None,
    quality: int | None,
    source_url: str | None,
) -> State:
    return {
        "bucket": output_bucket,
        "output_prefix": output_prefix,
        "status": None,
        "error": None,
        "datetime_started": time.mktime(start.timetuple()),
        "convert_time": None,
        "total_time": None,
        "attempts": 0,
        "version": settings.VERSION,
        "outputs": [],
        "output_format": output_format,
        "dpi": dpi,
        "page": page,
        "quality": quality,
        "source_url": source_url,
        "target_files": [],
    }


def _archive_outputs(
    page_files: list[tuple[str, str]],
    output_prefix: str,
    name_root: str,
    output_bucket: str,
) -> dict[str, Any]:
    """Create zip archive from converted page files and upload it to S3."""
    with NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
        with zipfile.ZipFile(tmp_zip.name, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path, arcname in page_files:
                zf.write(file_path, arcname=arcname)

    zip_key = f"{output_prefix.rstrip('/')}/{name_root}.zip"
    s3.upload_file(
        tmp_zip.name,
        output_bucket,
        zip_key,
        ExtraArgs={"ContentType": "application/zip"},
    )

    size = os.path.getsize(tmp_zip.name)
    os.remove(tmp_zip.name)
    return {"name": f"{name_root}.zip", "size": size}


def _resolve_output_location(event: Event) -> tuple[str, str]:
    output_bucket = event.get("output_bucket")
    output_prefix = event.get("output_prefix")

    if not output_bucket or not output_prefix:
        raise ValueError("output_bucket and output_prefix are required")

    return output_bucket, output_prefix


def _resolve_page(event: Event) -> int | None:
    page = event.get("page")
    if page is None:
        return None

    try:
        page_int = int(page)
    except (TypeError, ValueError):
        raise ValueError("page must be an integer")

    if page_int <= 0:
        raise ValueError(f"page must be >= 1, got {page_int}")

    return page_int


def _resolve_filename(source_url: str | None) -> tuple[str, str]:
    base = os.path.basename((source_url or "").split("?")[0])
    filename = base or "document.pdf"
    name_root, _ = os.path.splitext(filename)
    return filename, name_root


def _cleanup_temp_files(page_files: list[tuple[str, str]]) -> None:
    for file_path, _ in page_files:
        if os.path.exists(file_path):
            os.remove(file_path)


def _cleanup_input_file(input_file: Any | None) -> None:
    if not input_file:
        return

    try:
        input_file.close()
    except Exception:
        pass

    name = getattr(input_file, "name", None)
    if name and os.path.exists(name):
        os.remove(name)


def invoke(event: Event, _context: Any) -> State:
    start = datetime.utcnow()

    source_url: str | None = event.get("source_url")

    if not source_url:
        raise ValueError("source_url is required")

    output_bucket, output_prefix = _resolve_output_location(event)

    output_format = event.get("output_format", settings.DEFAULT_FORMAT)
    dpi = event.get("dpi", settings.DEFAULT_DPI)
    page = _resolve_page(event)
    quality = event.get("quality")

    _, name_root = _resolve_filename(source_url)

    state_key = state_file_key(output_prefix)
    state = _create_initial_state(
        start=start,
        output_bucket=output_bucket,
        output_prefix=output_prefix,
        output_format=output_format,
        dpi=dpi,
        page=page,
        quality=quality,
        source_url=source_url,
    )

    force: bool = event.get("force", False)
    prev_state = _load_state(output_bucket, state_key)

    if prev_state and not force:
        if prev_state.get("version") != settings.VERSION:
            prev_state["attempts"] = 0

        prev_status: Status = prev_state.get("status")  # type: ignore[assignment]
        state["attempts"] = prev_state.get("attempts", 0)

        if prev_status == "success" and _first_output_exists(prev_state):
            return prev_state

        if prev_status == "pending":
            if not _has_timed_out(prev_state, datetime.utcnow()):
                return prev_state

            state["attempts"] += 1
            state["status"] = "fail"
            state["error"] = "task timed out"
            _save_state(output_bucket, state_key, state)

        if (
            prev_status == "fail"
            and prev_state.get("attempts", 0) >= settings.MAX_ATTEMPTS
        ):
            return prev_state

    try:
        input_file = download_input_url(source_url)
    except Exception as exc:
        input_file = None
        state["status"] = "fail"
        state["error"] = str(exc)

    if input_file is None:
        state["status"] = state.get("status") or "fail"
        state["error"] = state.get("error") or "input file not found"
        state["total_time"] = (datetime.utcnow() - start).total_seconds()
        state["attempts"] += 1
        _save_state(output_bucket, state_key, state)
        return state

    state["status"] = "pending"
    _save_state(output_bucket, state_key, state)

    page_files_temp: list[tuple[str, str]] = []
    target_files: list[TargetFile] = []

    try:
        outputs, fmt = convert_pdf(
            input_file.name,
            output_format=output_format,
            dpi=dpi,
            page=page,
            quality=quality,
        )

        state["convert_time"] = (datetime.utcnow() - start).total_seconds()

        for page_number, image_path in outputs:
            key = build_output_key(output_prefix, page_number, fmt)
            target_filename = f"{name_root}-{page_number}.{fmt}"
            size = os.path.getsize(image_path)

            target_files.append(
                {
                    "id": len(target_files) + 1,
                    "name": target_filename,
                    "size": size,
                }
            )

            s3.upload_file(
                image_path,
                output_bucket,
                key,
                ExtraArgs={"ContentType": CONTENT_TYPES[fmt]},
            )

            state["outputs"].append({"page": page_number, "key": key})
            page_files_temp.append((image_path, target_filename))

        if len(page_files_temp) > 1:
            zip_info = _archive_outputs(
                page_files=page_files_temp,
                output_prefix=output_prefix,
                name_root=name_root,
                output_bucket=output_bucket,
            )
            target_files.append(
                {
                    "id": len(target_files) + 1,
                    **zip_info,
                }
            )

        state["target_files"] = target_files
        state["status"] = "success"
        state["attempts"] = 0

    except Exception as exc:
        state["status"] = "fail"
        state["attempts"] += 1
        state["error"] = str(exc)

    finally:
        state["total_time"] = (datetime.utcnow() - start).total_seconds()
        _save_state(output_bucket, state_key, state)

        _cleanup_temp_files(page_files_temp)
        _cleanup_input_file(input_file)

    return state


if settings.ROLLBAR_TOKEN:
    rollbar.init(
        environment=settings.ENVIRONMENT,
        access_token=settings.ROLLBAR_TOKEN,
    )


def handler(event, context):
    try:
        return invoke(event, context)
    except Exception:
        rollbar.report_exc_info(sys.exc_info())
        raise
