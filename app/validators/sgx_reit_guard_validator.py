"""Whole-database invariant guard for prod sgx_reit_* tables, via Supabase REST."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from ..database.connection import get_supabase_client

FAIL, WARN = "FAIL", "WARN"

PROPERTY_CATEGORY = {"Industrial & Logistics", "Office", "Retail", "Data Centers",
                      "Specialized", "Diversified (Commercial)"}
PROPERTY_STATUS = {"active", "divested", "held_for_sale"}
LAND_TENURE = {"Freehold", "Leasehold"}
PCT_BASIS = {"headline_rent", "annualised_rent", "npi", "asset_value",
             "gross_rental_income", "gross_revenue"}
BASIS_SEGMENT = {"office", "retail", "commercial", "logistics_industrial"}

PCT01 = [
    ("sgx_reit_property", "occupancy_rate"),
    ("sgx_reit_property", "ownership"),
    ("sgx_reit_top_tenant", "pct"),
    ("sgx_reit_trade_mix", "pct"),
    ("sgx_reit_property_transaction", "interest_pct"),
    ("sgx_reit_performance", "aggregate_leverage"),
    ("sgx_reit_performance", "cost_of_debt"),
    ("sgx_reit_performance", "portfolio_occupancy"),
]
NOT_PCT = [
    ("sgx_reit_performance", "interest_coverage_ratio", 0.5, 60),
    ("sgx_reit_performance", "weighted_average_lease_expiry", 0.1, 60),
    ("sgx_reit_performance", "weighted_average_debt_maturity", 0.1, 30),
    ("sgx_reit_performance", "distribution_per_unit", 0, 200),
    ("sgx_reit_performance", "net_asset_value_per_unit", 0, 50),
]


class SgxReitGuardValidator:
    """Read-only. Runs no writes against Supabase -- select/count only."""

    def __init__(self):
        self.supabase = get_supabase_client()
        self.findings: List[Dict[str, Any]] = []
        self.passes: List[Dict[str, Any]] = []

    def _add(self, severity: str, group: str, check: str, message: str):
        self.findings.append({"severity": severity, "group": group, "check": check, "message": message})

    def _ok(self, group: str, check: str, note: str = ""):
        self.passes.append({"group": group, "check": check, "note": note})

    def _fetch_all(self, table: str, columns: str = "*") -> List[Dict[str, Any]]:
        """Paginate through a table in pages of 1000 (Supabase REST's default page cap)."""
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

    def check_scale(self):
        g = "scale"
        for t, c in PCT01:
            rows = self._fetch_all(t, c)
            vals = [r[c] for r in rows if r.get(c) is not None]
            bad = [v for v in vals if v < 0 or v > 1]
            if bad:
                self._add(FAIL, g, f"{t}.{c}",
                           f"{len(bad)} rows outside 0-1 (min {min(bad)}, max {max(bad)}). "
                           f"Percentages are stored 0-1; a 0-100 value means the pct scale did not run")
            else:
                self._ok(g, f"{t}.{c}", f"{min(vals) if vals else 'n/a'} .. {max(vals) if vals else 'n/a'}")

        for t, c, lo, hi in NOT_PCT:
            rows = self._fetch_all(t, c)
            vals = [r[c] for r in rows if r.get(c) is not None]
            bad = [v for v in vals if v < lo or v > hi]
            if bad:
                self._add(WARN, g, f"{t}.{c}",
                           f"{len(bad)} rows outside the plausible band {lo}..{hi}. "
                           f"This column is NOT a percentage and must not be scaled")
            else:
                self._ok(g, f"{t}.{c}", "in band")

    def check_enums(self):
        g = "enums"
        ENUMS = [
            ("sgx_reit_property", "category", PROPERTY_CATEGORY),
            ("sgx_reit_property", "status", PROPERTY_STATUS),
            ("sgx_reit_property", "land_tenure", LAND_TENURE),
            ("sgx_reit_top_tenant", "pct_basis", PCT_BASIS),
            ("sgx_reit_top_tenant", "basis_segment", BASIS_SEGMENT),
            ("sgx_reit_trade_mix", "pct_basis", PCT_BASIS),
            ("sgx_reit_trade_mix", "basis_segment", BASIS_SEGMENT),
        ]
        for t, c, allowed in ENUMS:
            rows = self._fetch_all(t, c)
            vals = [r[c] for r in rows if r.get(c) is not None]
            distinct = set(vals)
            bad = distinct - allowed
            if bad:
                self._add(FAIL, g, f"{t}.{c}",
                           "values outside the canonical list: " + ", ".join(repr(v) for v in sorted(bad)))
            else:
                self._ok(g, f"{t}.{c}", f"{len(distinct)} distinct, all canonical")

    def check_sums(self):
        g = "sums"
        rows = self._fetch_all("sgx_reit_trade_mix", "symbol,financial_year,basis_segment,pct")
        by_scope: Dict[tuple, List[float]] = {}
        for r in rows:
            if r.get("pct") is None:
                continue
            key = (r["symbol"], r["financial_year"])
            by_scope.setdefault(key, []).append(float(r["pct"]))
        by_segment: Dict[tuple, float] = {}
        for r in rows:
            if r.get("pct") is None:
                continue
            key = (r["symbol"], r["financial_year"], r.get("basis_segment") or "-")
            by_segment[key] = by_segment.get(key, 0.0) + float(r["pct"]) * 100

        by_scope_total: Dict[tuple, Dict[str, float]] = {}
        for (sym, fy, seg), total in by_segment.items():
            by_scope_total.setdefault((sym, fy), {})[seg] = total

        bad = []
        for (sym, fy), segs in by_scope_total.items():
            each = all(abs(v - 100) <= 2 for v in segs.values())
            together = abs(sum(segs.values()) - 100) <= 2
            if not (each or together):
                bad.append((sym, fy, ", ".join(f"{s}={v:.1f}" for s, v in segs.items())))
        if bad:
            self._add(FAIL, g, "trade_mix.pct sums to 100",
                       "; ".join(f"{sym} FY{fy} [{d}]" for sym, fy, d in bad[:8]) +
                       (f" (+{len(bad) - 8} more)" if len(bad) > 8 else "") +
                       ". Each segment should sum to 100, or all segments together should")
        else:
            self._ok(g, "trade_mix.pct", f"{len(by_scope_total)} REIT-years sum correctly")

        rows2 = self._fetch_all("sgx_reit_top_tenant", "symbol,financial_year,basis_segment,pct")
        by_tenant_scope: Dict[tuple, float] = {}
        for r in rows2:
            if r.get("pct") is None:
                continue
            key = (r["symbol"], r["financial_year"], r.get("basis_segment") or "-")
            by_tenant_scope[key] = by_tenant_scope.get(key, 0.0) + float(r["pct"]) * 100
        bad2 = [(k, v) for k, v in by_tenant_scope.items() if v > 100.5]
        if bad2:
            self._add(FAIL, g, "top_tenant.pct <= 100 per segment",
                       "; ".join(f"{s} FY{fy} seg={seg} = {v:.1f}" for (s, fy, seg), v in bad2[:8]))
        else:
            self._ok(g, "top_tenant.pct", f"{len(by_tenant_scope)} scopes all <= 100")

        perf = self._fetch_all("sgx_reit_performance", "symbol,financial_year,net_property_income,gross_revenue")
        n = sum(1 for r in perf if r.get("net_property_income") is not None and r.get("gross_revenue") is not None
                and r["net_property_income"] > r["gross_revenue"])
        if n:
            self._add(FAIL, g, "performance NPI <= gross_revenue", f"{n} rows violate it")
        else:
            self._ok(g, "performance NPI <= gross_revenue")

        prop = self._fetch_all("sgx_reit_property", "symbol,financial_year,property_name,net_property_income,gross_revenue")
        n2 = sum(1 for r in prop if r.get("net_property_income") is not None and r.get("gross_revenue") is not None
                 and r["net_property_income"] > r["gross_revenue"])
        if n2:
            self._add(WARN, g, "property NPI <= gross_revenue",
                       f"{n2} rows violate it (prod has no currency tags, so a cross-currency figure "
                       f"cannot be ruled out -- kept as WARN, not FAIL)")
        else:
            self._ok(g, "property NPI <= gross_revenue")

    def check_keys(self):
        g = "keys"

        def dup_count(rows, key_fn):
            seen: Dict[tuple, int] = {}
            for r in rows:
                k = key_fn(r)
                seen[k] = seen.get(k, 0) + 1
            return sum(1 for v in seen.values() if v > 1)

        checks = [
            ("sgx_reit_property", "symbol,financial_year,property_name",
             lambda r: (r["symbol"], r["financial_year"], r["property_name"])),
            ("sgx_reit_top_tenant", "symbol,financial_year,rank",
             lambda r: (r["symbol"], r["financial_year"], r["rank"])),
            ("sgx_reit_trade_mix", "symbol,financial_year,category,pct_basis,basis_segment",
             lambda r: (r["symbol"], r["financial_year"], r["category"], r["pct_basis"], r.get("basis_segment"))),
            ("sgx_reit_performance", "symbol,financial_year",
             lambda r: (r["symbol"], r["financial_year"])),
        ]
        for table, cols, key_fn in checks:
            rows = self._fetch_all(table, cols)
            dup = dup_count(rows, key_fn)
            if dup:
                self._add(FAIL, g, f"{table} PK", f"{dup} duplicate keys on ({cols})")
            else:
                self._ok(g, f"{table} PK", "unique")

        rows = self._fetch_all("sgx_reit_property_transaction", "deal_id")
        counts: Dict[str, int] = {}
        for r in rows:
            if r.get("deal_id") is not None:
                counts[r["deal_id"]] = counts.get(r["deal_id"], 0) + 1
        singletons = [k for k, v in counts.items() if v == 1]
        if singletons:
            self._add(FAIL, g, "singleton deal_id",
                       f"{len(singletons)} deal_id values (non-null) group only a single row, which means "
                       f"broken/incomplete pairing metadata for an aggregated deal: " + ", ".join(singletons[:5]))
        else:
            self._ok(g, "singleton deal_id", f"{len(counts)} non-null deal_ids, all group >=2 rows")

        rows2 = self._fetch_all("sgx_reit_property_transaction",
                                 "deal_id,financial_year")
        by_deal: Dict[str, set] = {}
        for r in rows2:
            if r.get("deal_id") is not None:
                by_deal.setdefault(r["deal_id"], set()).add(r["financial_year"])
        cross_year = [k for k, fys in by_deal.items() if len(fys) > 1]
        if cross_year:
            self._add(FAIL, g, "transaction cross-year duplicates",
                       f"{len(cross_year)} deals appear in more than one financial year: " + ", ".join(cross_year[:5]))
        else:
            self._ok(g, "transaction cross-year duplicates", "none")

    def check_nulls(self):
        g = "nulls"
        REQ = [
            ("sgx_reit_property", ["symbol", "financial_year", "property_name", "category", "country"]),
            ("sgx_reit_top_tenant", ["symbol", "financial_year", "rank", "client_name"]),
            ("sgx_reit_trade_mix", ["symbol", "financial_year", "category", "pct_basis"]),
            ("sgx_reit_performance", ["symbol", "financial_year"]),
            ("sgx_reit_property_transaction", ["symbol", "financial_year", "transaction_type", "property_name"]),
        ]
        clean = True
        for t, cols in REQ:
            rows = self._fetch_all(t, ",".join(cols))
            for c in cols:
                n = sum(1 for r in rows if r.get(c) is None)
                if n:
                    clean = False
                    self._add(FAIL, g, f"{t}.{c}", f"null on {n} rows; required for downstream use")
        if clean:
            self._ok(g, "required columns", "no nulls")

    def check_tallies(self):
        g = "tallies"
        rows = self._fetch_all(
            "sgx_reit_performance",
            "symbol,financial_year,distributable_income_opening,income_for_year,other_additions,"
            "distribution_paid,amount_retained,distributable_income_closing,distribution_record,distribution_per_unit")

        tally = miss = 0
        bad = []
        for r in rows:
            o, i, c = r.get("distributable_income_opening"), r.get("income_for_year"), r.get("distributable_income_closing")
            if o is None or i is None or c is None:
                miss += 1
                continue
            a = r.get("other_additions") or 0
            p = r.get("distribution_paid") or 0
            ret = r.get("amount_retained") or 0
            calc = float(o) + float(i) + float(a) - float(p) - float(ret)
            if abs(calc - float(c)) > 1:
                bad.append((r["symbol"], r["financial_year"], round(calc), float(c)))
            else:
                tally += 1
        if bad:
            self._add(WARN, g, "distribution rollforward",
                       f"{len(bad)} rows do not close: " +
                       "; ".join(f"{s} FY{fy} calc {c1:,} vs closing {c2:,.0f}" for s, fy, c1, c2 in bad[:5]))
        self._ok(g, "distribution rollforward", f"{tally} close, {miss} have a null input, {len(bad)} off")

        off = []
        for r in rows:
            rec, dpu = r.get("distribution_record"), r.get("distribution_per_unit")
            if not rec or dpu is None:
                continue
            total = sum(t.get("dpu") for t in rec if t.get("dpu") is not None)
            if abs(total - float(dpu)) > 0.02:
                off.append((r["symbol"], r["financial_year"], round(total, 3), float(dpu)))
        if off:
            self._add(WARN, g, "sum(distribution_record.dpu) = distribution_per_unit",
                       f"{len(off)} rows short: " +
                       "; ".join(f"{s} FY{fy} record {a} vs total {b}" for s, fy, a, b in off[:6]) +
                       ". Usually a semi-annual payer with only one half captured")
        else:
            self._ok(g, "sum(distribution_record.dpu) = distribution_per_unit", "all tally")

    def check_coverage(self):
        g = "coverage"
        spine = {(r["symbol"], r["financial_year"])
                  for r in self._fetch_all("sgx_reit_performance", "symbol,financial_year")}
        for t in ("sgx_reit_property", "sgx_reit_top_tenant", "sgx_reit_trade_mix", "sgx_reit_property_transaction"):
            rows = self._fetch_all(t, "symbol,financial_year")
            keys = {(r["symbol"], r["financial_year"]) for r in rows}
            orphan = sorted(keys - spine)
            if orphan:
                self._add(FAIL, g, t, f"{len(orphan)} REIT-years have no sgx_reit_performance row: " +
                           ", ".join(f"{s} FY{fy}" for s, fy in orphan[:6]))
            else:
                self._ok(g, t, f"{len(keys)} REIT-years all present in performance")

    def check_segments(self):
        g = "segments"
        for t in ("sgx_reit_top_tenant", "sgx_reit_trade_mix"):
            rows = self._fetch_all(t, "symbol,financial_year,basis_segment")
            by_sym: Dict[str, Dict[int, tuple]] = {}
            for r in rows:
                sym, fy = r["symbol"], r["financial_year"]
                by_sym.setdefault(sym, {}).setdefault(fy, [0, 0])
                by_sym[sym][fy][1] += 1
                if r.get("basis_segment"):
                    by_sym[sym][fy][0] += 1

            flagged = []
            for sym, years in by_sym.items():
                tagged_years = {fy for fy, (seg, _n) in years.items() if seg}
                if not tagged_years:
                    continue
                max_n = max(n for _seg, n in years.values())
                for fy, (seg, n) in years.items():
                    if seg == 0 and n >= 0.8 * max_n:
                        flagged.append(f"{sym} FY{fy} ({n} rows, none tagged; FY{sorted(tagged_years)[0]} is)")
            if flagged:
                self._add(WARN, g, t, "segments tagged in one year but not another: " + "; ".join(flagged[:6]))
            else:
                self._ok(g, t, "segment tagging consistent across years")

    GROUPS = {
        "scale": check_scale,
        "enums": check_enums,
        "sums": check_sums,
        "keys": check_keys,
        "nulls": check_nulls,
        "tallies": check_tallies,
        "coverage": check_coverage,
        "segments": check_segments,
    }

    async def validate(self, only: Optional[List[str]] = None) -> Dict[str, Any]:
        selected = only if only else list(self.GROUPS.keys())
        unknown = [g for g in selected if g not in self.GROUPS]
        if unknown:
            return {
                "table_name": "sgx_reit_guard",
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
            "table_name": "sgx_reit_guard",
            "validation_timestamp": datetime.utcnow().isoformat(),
            "groups_checked": selected,
            "total_rows": len(self.passes) + len(self.findings),
            "anomalies_count": len(anomalies),
            "anomalies": anomalies,
            "status": "flagged" if any(f["severity"] == FAIL for f in self.findings) else "success",
            "validations_performed": selected,
        }
