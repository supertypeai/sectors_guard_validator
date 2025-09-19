from fastapi import APIRouter, Request, Header, HTTPException, status
from fastapi.responses import PlainTextResponse, JSONResponse, FileResponse, Response
import os
from pathlib import Path
import time
import csv
import asyncio
import httpx
from typing import Optional
from datetime import datetime, timezone

router = APIRouter()

# Storage directory for cached sheet
DATA_DIR = Path(__file__).resolve().parents[2] / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
SHEET_PATH = DATA_DIR / "sheet.csv"
META_PATH = DATA_DIR / "sheet.meta"


def _write_meta(meta: dict):
    try:
        with META_PATH.open("w", encoding="utf-8") as f:
            for k, v in meta.items():
                f.write(f"{k}={v}\n")
    except Exception:
        pass


def _read_meta() -> dict:
    meta = {}
    if META_PATH.exists():
        try:
            with META_PATH.open("r", encoding="utf-8") as f:
                for line in f:
                    if "=" in line:
                        k, v = line.strip().split("=", 1)
                        meta[k] = v
        except Exception:
            return {}
    return meta


async def _fetch_and_save(csv_url: str, timeout: int = 30) -> dict:
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(csv_url, follow_redirects=True)
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Failed to fetch sheet: {resp.status_code}")
        # Guard: if the response is HTML (login page, error page, or incorrect URL), don't save it as CSV
        content_type = (resp.headers.get("content-type") or "").lower()
        # Try to detect HTML in content-type or body preview
        text_preview = ""
        try:
            text_preview = resp.text or ""
        except Exception:
            text_preview = ""

        looks_like_html = False
        if "html" in content_type:
            looks_like_html = True
        else:
            low = text_preview.lstrip().lower()
            if low.startswith("<!doctype") or low.startswith("<html") or "<html" in low or "<body" in low or "<table" in low:
                looks_like_html = True

        if looks_like_html:
            # don't write the HTML to disk; return a helpful error
            preview = (text_preview[:300] + "...") if len(text_preview) > 300 else text_preview
            raise HTTPException(
                status_code=502,
                detail=(
                    f"Fetched content appears to be HTML (content-type={content_type}). "
                    "Make sure the URL points to a CSV export (e.g. Google Sheets export URL) or that the sheet is publicly accessible. "
                    f"Preview: {preview}"
                ),
            )

        # write bytes to file
        SHEET_PATH.write_bytes(resp.content)

    # compute rows
    try:
        text = resp.text
        rows = text.replace('\r', '').split('\n')
        # remove possible trailing empty line
        if rows and rows[-1] == "":
            rows = rows[:-1]
        row_count = len(rows)
    except Exception:
        row_count = -1

    meta = {"timestamp": str(int(time.time())), "rows": str(row_count), "source": csv_url}
    _write_meta(meta)
    return meta


def _sheet_modified_today() -> bool:
    """Return True if SHEET_PATH exists and its last modified date is today (UTC)."""
    try:
        if not SHEET_PATH.exists():
            return False
        mtime = SHEET_PATH.stat().st_mtime
        dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
        today = datetime.now(timezone.utc).date()
        return dt.date() == today
    except Exception:
        return False


@router.post("/internal/fetch-sheet")
async def trigger_fetch(request: Request, authorization: Optional[str] = Header(None)):
    """Protected endpoint to trigger backend to fetch the Google Sheet.

    Expects Authorization: Bearer <token> header. The backend reads env GSHEET_CSV_URL if no csv_url passed in JSON body.
    """
    # simple auth using BACKEND_API_TOKEN env var
    expected = os.getenv("BACKEND_API_TOKEN")
    if expected:
        if not authorization or not authorization.lower().startswith("bearer "):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing or invalid Authorization header")
        token = authorization.split(None, 1)[1].strip()
        if token != expected:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid trigger token")

    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
    except Exception:
        # Could be invalid JSON; treat as empty dict
        body = {}

    csv_url = body.get("csv_url")
    mode = (body.get("mode") or "").strip().lower()

    if not csv_url:
        csv_url = os.getenv("GSHEET_CSV_URL")

    if not csv_url:
        raise HTTPException(status_code=400, detail="No CSV URL provided and GSHEET_CSV_URL not set in backend environment")

    # If the sheet has already been fetched/updated today, skip re-downloading unless forced
    if mode != "force" and _sheet_modified_today():
        meta = _read_meta()
        print("Skipping fetch; sheet already fresh today")
        return JSONResponse({"ok": True, "skipped": True, "reason": "already_fresh_today", "meta": meta})

    # fetch and save (forced or stale/missing)
    try:
        meta = await _fetch_and_save(csv_url)
        print(f"Fetched and saved sheet data: {meta}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return JSONResponse({"ok": True, "meta": meta})


@router.get("/api/sheet")
async def get_sheet(format: Optional[str] = None):
    """Return cached sheet. If format=json, parse CSV and return JSON array (list of rows as dicts if header present).
    Otherwise returns raw CSV (text/csv).
    """
    if not SHEET_PATH.exists():
        # 204 No Content MUST NOT include a response body. Returning a JSON body with 204
        # causes a Content-Length mismatch under some ASGI servers/proxies.
        # See: https://www.rfc-editor.org/rfc/rfc9110#name-204-no-content
        return Response(status_code=204)

    if format and format.lower() == "json":
        try:
            text = SHEET_PATH.read_text(encoding="utf-8")
            reader = csv.DictReader(text.replace('\r', '').split('\n'))
            rows = [r for r in reader]
            return JSONResponse({"ok": True, "rows": len(rows), "data": rows})
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    # default: return file
    return FileResponse(path=str(SHEET_PATH), media_type="text/csv", filename="sheet.csv")
