from fastapi import APIRouter, Request, Header, HTTPException, status
from fastapi.responses import JSONResponse, FileResponse, Response
import os
from pathlib import Path
import time
import csv
import httpx
from typing import Optional, Dict
from datetime import datetime, timezone

router = APIRouter()

# Storage directory for cached sheets
DATA_DIR = Path(__file__).resolve().parents[2] / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Sheet registry: name -> { env_var, file, meta }
SHEET_REGISTRY: Dict[str, Dict[str, str]] = {
    "workflows": {
        "env_var": "GSHEET_CSV_URL",
        "file": str(DATA_DIR / "workflows.csv"),
        "meta": str(DATA_DIR / "workflows.meta"),
    },
    "cron": {
        "env_var": "GSHEET_CSV_URL_CRON",
        "file": str(DATA_DIR / "cron.csv"),
        "meta": str(DATA_DIR / "cron.meta"),
    },
}


def _resolve_sheet(name: str) -> Dict[str, str]:
    if name not in SHEET_REGISTRY:
        raise HTTPException(status_code=400, detail=f"Unknown sheet '{name}'. Known: {sorted(SHEET_REGISTRY.keys())}")
    return SHEET_REGISTRY[name]


def _write_meta(path: str, meta: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            for k, v in meta.items():
                f.write(f"{k}={v}\n")
    except Exception:
        pass


def _read_meta(path: str) -> dict:
    meta = {}
    if not os.path.exists(path):
        return meta
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    meta[k] = v
    except Exception:
        return {}
    return meta


def _sheet_modified_today(path: str) -> bool:
    try:
        if not os.path.exists(path):
            return False
        mtime = os.path.getmtime(path)
        dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
        today = datetime.now(timezone.utc).date()
        return dt.date() == today
    except Exception:
        return False


async def _fetch_and_save(csv_url: str, file_path: str, meta_path: str, timeout: int = 30) -> dict:
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(csv_url, follow_redirects=True)
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Failed to fetch sheet: {resp.status_code}")
        content_type = (resp.headers.get("content-type") or "").lower()
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
            preview = (text_preview[:300] + "...") if len(text_preview) > 300 else text_preview
            raise HTTPException(
                status_code=502,
                detail=(
                    f"Fetched content appears to be HTML (content-type={content_type}). "
                    "Make sure the URL points to a CSV export (e.g. Google Sheets export URL) or that the sheet is publicly accessible. "
                    f"Preview: {preview}"
                ),
            )

        Path(file_path).write_bytes(resp.content)

    try:
        text = resp.text
        rows = text.replace('\r', '').split('\n')
        if rows and rows[-1] == "":
            rows = rows[:-1]
        row_count = len(rows)
    except Exception:
        row_count = -1

    meta = {"timestamp": str(int(time.time())), "rows": str(row_count), "source": csv_url}
    _write_meta(meta_path, meta)
    return meta


async def ensure_sheet_cache_on_start() -> None:
    """Optionally prefetch known sheets on application startup when cache is missing.
    Skips a sheet if its cache already exists for today.
    """
    try:
        prefetch_flag = (os.getenv("PREFETCH_SHEET_ON_START", "").strip().lower() in {"1", "true"})
        if not prefetch_flag:
            return
        import asyncio

        async def _maybe_fetch(name: str, cfg: Dict[str, str]) -> None:
            try:
                if _sheet_modified_today(cfg["file"]):
                    return
                csv_url = os.getenv(cfg["env_var"])
                if not csv_url:
                    print(f"[startup] PREFETCH enabled but {cfg['env_var']} is not set; skipping {name}")
                    return
                print(f"[startup] Prefetching {name} sheet cache...")
                await _fetch_and_save(csv_url, cfg["file"], cfg["meta"])
                print(f"[startup] {name} prefetch completed")
            except HTTPException as he:
                print(f"[startup] {name} prefetch failed: {he.detail}")
            except Exception as e:
                print(f"[startup] {name} prefetch error: {e}")

        await asyncio.gather(*(_maybe_fetch(name, cfg) for name, cfg in SHEET_REGISTRY.items()))
    except Exception as outer:
        print(f"[startup] ensure_sheet_cache_on_start error: {outer}")


def _check_auth(authorization: Optional[str]) -> None:
    expected = os.getenv("BACKEND_API_TOKEN")
    if not expected:
        return
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing or invalid Authorization header")
    parts = authorization.split(None, 1)
    if len(parts) != 2 or not parts[1].strip():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing or invalid Authorization header")
    token = parts[1].strip()
    if token != expected:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid trigger token")


@router.post("/internal/fetch-sheet")
async def trigger_fetch(request: Request, authorization: Optional[str] = Header(None)):
    """Trigger a fetch for a named sheet. Body: { sheet?: string, csv_url?: string, mode?: "force" }.
    Default sheet is "workflows" for backward compat with the existing fetch-sheet.yml workflow.
    """
    _check_auth(authorization)

    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
    except Exception:
        body = {}

    sheet_name = (body.get("sheet") or "workflows").strip().lower()
    cfg = _resolve_sheet(sheet_name)

    csv_url = (body.get("csv_url") or os.getenv(cfg["env_var"]) or "").strip()
    mode = (body.get("mode") or "").strip().lower()

    if not csv_url:
        raise HTTPException(status_code=400, detail=f"No CSV URL provided and {cfg['env_var']} not set in backend environment")
    if not csv_url.lower().startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail=f"Invalid CSV URL for sheet '{sheet_name}': must start with http:// or https://")

    if mode != "force" and _sheet_modified_today(cfg["file"]):
        meta = _read_meta(cfg["meta"])
        print(f"[fetch-sheet] Skipping {sheet_name}; already fresh today")
        return JSONResponse({"ok": True, "skipped": True, "reason": "already_fresh_today", "sheet": sheet_name, "meta": meta})

    try:
        meta = await _fetch_and_save(csv_url, cfg["file"], cfg["meta"])
        print(f"[fetch-sheet] Fetched and saved {sheet_name}: {meta}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return JSONResponse({"ok": True, "sheet": sheet_name, "meta": meta})


@router.get("/api/sheet")
async def get_sheet(format: Optional[str] = None, sheet: Optional[str] = None):
    """Return cached sheet. If sheet is omitted, defaults to 'workflows'."""
    sheet_name = (sheet or "workflows").strip().lower()
    cfg = _resolve_sheet(sheet_name)

    if not os.path.exists(cfg["file"]):
        return Response(status_code=204)

    if format and format.lower() == "json":
        try:
            text = Path(cfg["file"]).read_text(encoding="utf-8")
            reader = csv.DictReader(text.replace('\r', '').split('\n'))
            rows = [r for r in reader]
            return JSONResponse({"ok": True, "sheet": sheet_name, "rows": len(rows), "data": rows})
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    return FileResponse(path=cfg["file"], media_type="text/csv", filename=f"{sheet_name}.csv")
