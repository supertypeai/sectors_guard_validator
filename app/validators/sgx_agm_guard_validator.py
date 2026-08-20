"""Whole-table invariant guard for prod sgx_agm, via Supabase REST."""

import re
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from ..database.connection import get_supabase_client

FAIL, WARN = "FAIL", "WARN"

MEETING_TYPE = {"AGM", "EGM"}
PLACE_DESC = {"Onsite", "Hybrid", "Online"}
TAGS = {"dividend", "financial reporting and profit allocation", "board and management",
        "capital and equity", "corporate governance", "acquisitions and disposals"}
SYMBOL_RE = re.compile(r"^[A-Z0-9]{3,4}$")
SOURCE_PREFIX = "https://links.sgx.com/1.0.0/corporate-announcements/"


class SgxAgmGuardValidator:
    """Read-only. Runs no writes against Supabase -- select/count only."""

    def __init__(self):
        self.supabase = get_supabase_client()
        self.findings: List[Dict[str, Any]] = []
        self.passes: List[Dict[str, Any]] = []
        self._rows: Optional[List[Dict[str, Any]]] = None

    def _add(self, severity: str, group: str, check: str, message: str):
        self.findings.append({"severity": severity, "group": group, "check": check, "message": message})

    def _ok(self, group: str, check: str, note: str = ""):
        self.passes.append({"group": group, "check": check, "note": note})

    def _fetch_all(self, table: str, columns: str = "*") -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        offset = 0
        page_size = 1000
        while True:
            resp = self.supabase.table(table).select(columns).range(offset, offset + page_size - 1).execute()
            page = resp.data or []
            rows.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
        return rows

    def _agm(self) -> List[Dict[str, Any]]:
        if self._rows is None:
            self._rows = self._fetch_all("sgx_agm")
        return self._rows

    @staticmethod
    def _label(r: Dict[str, Any]) -> str:
        return f"{r.get('symbol')} {r.get('agm_date')}"

    @staticmethod
    def _as_date(value: Any) -> Optional[date]:
        if not value:
            return None
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            return None

    @staticmethod
    def _as_dt(value: Any) -> Optional[datetime]:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    def check_enums(self):
        g = "enums"
        rows = self._agm()

        bad = sorted({r["meeting_type"] for r in rows if r.get("meeting_type") not in MEETING_TYPE})
        if bad:
            self._add(FAIL, g, "meeting_type",
                      "values outside the closed set AGM/EGM: " + ", ".join(repr(v) for v in bad))
        else:
            self._ok(g, "meeting_type", f"{len({r['meeting_type'] for r in rows})} distinct, all canonical")

        vals = [r["agm_place_desc"] for r in rows if r.get("agm_place_desc") is not None]
        bad = sorted(set(vals) - PLACE_DESC)
        if bad:
            self._add(FAIL, g, "agm_place_desc",
                      "values outside the closed set Onsite/Hybrid/Online: " + ", ".join(repr(v) for v in bad))
        else:
            self._ok(g, "agm_place_desc", f"{len(set(vals))} distinct, all canonical")

        offenders = []
        for r in rows:
            for t in r.get("tags") or []:
                if t not in TAGS:
                    offenders.append(f"{self._label(r)}: {t!r}")
        if offenders:
            self._add(FAIL, g, "tags",
                      f"{len(offenders)} tag values outside the six-tag enum: " + "; ".join(offenders[:6]))
        else:
            self._ok(g, "tags", f"{len({t for r in rows for t in (r.get('tags') or [])})} distinct, all canonical")

    def check_format(self):
        g = "format"
        rows = self._agm()

        bad = sorted({r["symbol"] for r in rows if not SYMBOL_RE.match(r.get("symbol") or "")})
        if bad:
            self._add(FAIL, g, "symbol",
                      f"{len(bad)} symbols not matching the 3-4 char uppercase alnum pattern: "
                      + ", ".join(repr(v) for v in bad[:6]))
        else:
            self._ok(g, "symbol", f"{len({r['symbol'] for r in rows})} distinct, all 3-4 char uppercase alnum")

        bad_rows = [r for r in rows if not (r.get("source_link") or "").startswith(SOURCE_PREFIX)]
        if bad_rows:
            self._add(FAIL, g, "source_link",
                      f"{len(bad_rows)} rows whose source_link does not start with {SOURCE_PREFIX}: "
                      + "; ".join(self._label(r) for r in bad_rows[:6]))
        else:
            self._ok(g, "source_link", "all rows sourced from the SGX corporate-announcements endpoint")

    def check_consistency(self):
        g = "consistency"
        rows = self._agm()

        mismatched = [r for r in rows if (r.get("tags") is None) != (r.get("summary") is None)]
        if mismatched:
            self._add(FAIL, g, "tags_summary_pairing",
                      f"{len(mismatched)} rows where exactly one of tags/summary is populated; "
                      "both come from the same LLM call: " + "; ".join(self._label(r) for r in mismatched[:6]))
        else:
            self._ok(g, "tags_summary_pairing",
                     f"{sum(1 for r in rows if r.get('summary') is not None)} of {len(rows)} rows populated")

        today = datetime.now(timezone.utc).date()
        future = [r for r in rows if (self._as_date(r.get("agm_date")) or today) > today]
        premature = [r for r in future if r.get("summary") is not None]
        if premature:
            self._add(FAIL, g, "summary_before_meeting",
                      f"{len(premature)} rows carry a summary for a meeting that has not happened yet: "
                      + "; ".join(self._label(r) for r in premature[:6]))
        else:
            self._ok(g, "summary_before_meeting", f"{len(future)} future-dated rows, none summarised")

    def check_sias(self):
        g = "sias"
        rows = self._agm()

        for col in ("sias_response_pdf", "qa"):
            orphans = [r for r in rows if r.get(col) is not None and r.get("sias_questions_pdf") is None]
            if orphans:
                self._add(FAIL, g, col,
                          f"{len(orphans)} rows have {col} without sias_questions_pdf; "
                          "SIAS always publishes questions first: " + "; ".join(self._label(r) for r in orphans[:6]))
            else:
                self._ok(g, col, f"{sum(1 for r in rows if r.get(col) is not None)} populated, none orphaned")

    def check_freshness(self):
        g = "freshness"
        rows = self._agm()

        now = datetime.now(timezone.utc)
        future = [r for r in rows if (self._as_dt(r.get("updated_on")) or now) > now]
        if future:
            latest = max(self._as_dt(r["updated_on"]) for r in future)
            self._add(WARN, g, "updated_on",
                      f"{len(future)} rows stamped in the future (latest {latest.isoformat()}); "
                      "indicates a clock or load bug: " + "; ".join(self._label(r) for r in future[:6]))
        else:
            stamps = [s for s in (self._as_dt(r.get("updated_on")) for r in rows) if s]
            self._ok(g, "updated_on", f"latest {max(stamps).isoformat()}" if stamps else "no timestamps")

    GROUPS = {
        "enums": check_enums,
        "format": check_format,
        "consistency": check_consistency,
        "sias": check_sias,
        "freshness": check_freshness,
    }

    async def validate(self, only: Optional[List[str]] = None) -> Dict[str, Any]:
        selected = only if only else list(self.GROUPS.keys())
        unknown = [g for g in selected if g not in self.GROUPS]
        if unknown:
            return {
                "table_name": "sgx_agm_guard",
                "status": "error",
                "error": f"unknown group(s): {unknown}. Known: {list(self.GROUPS)}",
                "validation_timestamp": datetime.utcnow().isoformat(),
            }

        self.findings = []
        self.passes = []
        for name in selected:
            try:
                self.GROUPS[name](self)
            except Exception as exc:
                self._add(FAIL, name, "check crashed", f"{type(exc).__name__}: {exc}")

        anomalies = [
            {
                "type": f"{f['group']}.{f['check']}",
                "message": f["message"],
                "severity": "flagged" if f["severity"] == FAIL else "info",
            }
            for f in self.findings
        ]

        return {
            "table_name": "sgx_agm_guard",
            "validation_timestamp": datetime.utcnow().isoformat(),
            "groups_checked": selected,
            "total_rows": len(self.passes) + len(self.findings),
            "anomalies_count": len(anomalies),
            "anomalies": anomalies,
            "status": "flagged" if any(f["severity"] == FAIL for f in self.findings) else "success",
            "validations_performed": selected,
        }
