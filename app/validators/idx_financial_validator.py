import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import re
import asyncio
import pytz
import json

from .data_validator import DataValidator
from ..database.connection import get_supabase_client

class IDXFinancialValidator(DataValidator):
    """
    Specialized validator for IDX financial data tables
    """
    
    def __init__(self):
        super().__init__()
        self.supabase = get_supabase_client()
        self.idx_tables = {
            'idx_combine_financials_annual': self._validate_financial_annual,
            'idx_combine_financials_quarterly': self._validate_financial_quarterly,
            'idx_daily_data': self._validate_daily_data,
            'idx_daily_data_completeness': self._validate_daily_data_completeness_and_coverage,
            'index_daily_data': self._validate_index_daily_data,
            'idx_dividend': self._validate_dividend,
            'idx_all_time_price': self._validate_all_time_price,
            'idx_filings': self._validate_filings,
            'idx_stock_split': self._validate_stock_split,
            'idx_news': self._validate_news,
            'sgx_company_report': self._validate_sgx_company_report,
            'sgx_manual_input': self._validate_sgx_manual_input,
            'idx_company_profile': self._validate_company_profile
        }
    
    async def validate_table(self, table_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None, run_only_coverage: bool = False) -> Dict[str, Any]:
        """
        Override parent method to use IDX-specific validation rules with optional date filtering
        """
        if table_name not in self.idx_tables:
            # Fall back to generic validation for non-IDX tables
            return await super().validate_table(table_name)
        
        try:
            # Get table data with date filtering. Helper returns the applied start/end (may be defaulted)
            data, applied_start, applied_end = await self._fetch_table_data_with_filter(table_name, start_date, end_date)

            # Run IDX-specific validation
            results = {
                "table_name": table_name,
                "validation_timestamp": datetime.now().isoformat(),
                "total_rows": len(data),
                "anomalies_count": 0,
                "anomalies": [],
                "status": "success",
                "validations_performed": [f"idx_{table_name.split('_')[-1]}_validation"],
                "date_filter": {
                    "start_date": applied_start,
                    "end_date": applied_end
                } if applied_start or applied_end else None
            }
            
            if not data.empty:
                # Run table-specific validation
                if table_name == 'idx_daily_data' and run_only_coverage:
                    idx_results = await self._validate_daily_data_completeness_and_coverage(data)
                else:
                    validation_func = self.idx_tables[table_name]
                    idx_results = await validation_func(data)
                results["anomalies"].extend(idx_results.get("anomalies", []))
                if table_name == 'sgx_company_report':
                    results["sgx_top_50_filter"] = {
                        "applied": True,
                        "companies_validated": len(data),
                        "note": "Validation limited to top 50 companies by market capitalization"
                    }
            
            # Filter anomalies: only keep 'error' severity for database storage
            all_anomalies = results["anomalies"].copy()  # Keep all for return
            error_anomalies = [a for a in results["anomalies"] if a.get("severity") == "error"]
            results["anomalies"] = error_anomalies  # Only errors go to database
            
            # Update final counts and status based on error anomalies only
            results["anomalies_count"] = len(error_anomalies)
            
            if results["anomalies_count"] > 0:
                results["status"] = "error"
            
            # Store results (only errors)
            await self._store_validation_results(results)
            
            # Return all anomalies for API response
            results["anomalies"] = all_anomalies
            results["total_anomalies_found"] = len(all_anomalies)
            results["errors_stored"] = len(error_anomalies)
            
            return results
        except Exception as e:
            return {
                "table_name": table_name,
                "status": "error",
                "error": str(e),
                "validation_timestamp": datetime.now().isoformat()
            }
    async def _fetch_table_data_with_filter(self, table_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Tuple[pd.DataFrame, Optional[str], Optional[str]]:
        """Fetch data from Supabase table with optional date filtering and SGX top 50 filtering"""
        try:
            print(f"📊 [Validator] Fetching data from table: {table_name}")
            print(f"📅 [Validator] Date filter - Start: {start_date}, End: {end_date}")

            # Alias mapping: some validator entries are logical/pseudo tables mapped to a real table
            alias_map = {
                'idx_daily_data_completeness': 'idx_daily_data'
            }
            query_table = alias_map.get(table_name, table_name)

            jakarta_tz = pytz.timezone('Asia/Jakarta')
            today = pd.Timestamp(datetime.now(jakarta_tz).date())
            # Special default window for completeness: target only the previous business day
            if table_name == 'idx_daily_data_completeness' and not start_date and not end_date:
                # If today is Monday (0), go back 3 days to Friday; otherwise go back 1 day
                days = 3 if today.weekday() == 0 else 1
                target = (today - timedelta(days=days))
                start_date = target.isoformat()
                end_date = start_date
            if query_table == 'idx_daily_data' and not start_date and not end_date:
                start_date = (today - timedelta(days=7)).isoformat()
                end_date = (today - timedelta(days=1)).isoformat()
            elif query_table == 'index_daily_data' and not start_date and not end_date:
                start_date = (today - timedelta(days=7)).isoformat()
                end_date = today.isoformat()
            elif query_table == 'idx_combine_financials_quarterly' and not start_date and not end_date:
                start_date = (today - timedelta(days=365)).isoformat()
                end_date = today.isoformat()

            query = self.supabase.table(query_table).select("*")

            # Apply date filters at the database level when possible
            # Map known tables to their date/timestamp columns
            date_filter_column = None
            if query_table in {
                'idx_daily_data', 'index_daily_data', 'idx_combine_financials_quarterly',
                'idx_combine_financials_annual', 'idx_dividend', 'idx_all_time_price',
                'idx_stock_split'
            }:
                date_filter_column = 'date'
            elif query_table == 'idx_filings':
                date_filter_column = 'timestamp'

            # If we have a target column and a start/end, apply inclusive filters
            if date_filter_column and (start_date or end_date):
                try:
                    start_val = start_date
                    end_val = end_date
                    # For timestamp columns, widen to full-day range to be inclusive
                    if date_filter_column == 'timestamp':
                        if start_val:
                            start_val = f"{start_val}T00:00:00"
                        if end_val:
                            end_val = f"{end_val}T23:59:59"
                    if start_val:
                        query = query.gte(date_filter_column, start_val)
                    if end_val:
                        query = query.lte(date_filter_column, end_val)
                except Exception as err:
                    print(f"⚠️  [Validator] Failed to apply server-side date filters for {table_name}.{date_filter_column}: {err}")

            # Execute base query
            try:
                response = query.execute()
                raw_data = getattr(response, 'data', None)
                # print(f"🧪 [Validator] Raw response type={type(raw_data)} length={len(raw_data) if raw_data is not None else 'None'}")
            except Exception as err:
                print(f"❌ [Validator] Supabase query error for {query_table} (alias of {table_name}): {err}")
                raw_data = None
            df = pd.DataFrame(raw_data) if raw_data else pd.DataFrame()

            # Client-side top 50 by market cap only
            if table_name == 'sgx_company_report' and not df.empty:
                print(f"🏁 [Validator] Starting SGX top 50 filtering pipeline (rows={len(df)})")
                # Detect alternate market cap column names
                market_cap_col = None
                possible_cols = ['market_cap', 'marketCap', 'market_capitalization', 'mkt_cap', 'mcap', 'market_value']
                for c in possible_cols:
                    if c in df.columns:
                        market_cap_col = c
                        break
                if market_cap_col and market_cap_col != 'market_cap':
                    try:
                        df['market_cap'] = df[market_cap_col]
                        print(f"🔁 [Validator] Normalized market cap column '{market_cap_col}' -> 'market_cap'")
                    except Exception as err:
                        print(f"⚠️  [Validator] Failed to normalize market cap column '{market_cap_col}': {err}")

                if 'market_cap' not in df.columns:
                    print(f"⚠️  [Validator] SGX table missing market cap columns (checked {possible_cols}); cannot apply top 50 filter. Proceeding without filter.")
                else:
                    def _mc_to_number(v):
                        try:
                            if isinstance(v, (int, float)):
                                return float(v)
                            if isinstance(v, str):
                                cleaned = re.sub(r'[^0-9.\-]', '', v)
                                if cleaned.count('.') > 1:
                                    first, *rest = cleaned.split('.')
                                    cleaned = first + '.' + ''.join(rest)
                                return float(cleaned) if cleaned not in ['', '.', '-'] else np.nan
                            if isinstance(v, dict):
                                for k in ['market_cap', 'value', 'amount']:
                                    if k in v:
                                        return _mc_to_number(v[k])
                            return np.nan
                        except Exception:
                            return np.nan
                    try:
                        df['__mc_numeric'] = df['market_cap'].apply(_mc_to_number)
                        before_non_null = df['__mc_numeric'].notna().sum()
                        # Filter out null / non-positive market caps first
                        filtered = df[df['__mc_numeric'].notna() & (df['__mc_numeric'] > 0)]
                        if filtered.empty:
                            print("⚠️  [Validator] No positive market_cap values after parsing; skipping top 50 reduction.")
                        else:
                            filtered = filtered.sort_values('__mc_numeric', ascending=False, na_position='last')
                            top_n = filtered.head(50)
                            print(f"📈 [Validator] SGX top 50 selected (from {len(df)} -> {len(top_n)})")
                            df = top_n.drop(columns=['__mc_numeric'])
                    except Exception as err:
                        print(f"⚠️  [Validator] Error during SGX top 50 filtering: {err}. Proceeding with unfiltered data.")
            
            # Normalize common date aliases if present (so downstream validators can assume 'date')
            alias_date_cols = ['date', 'ex_date', 'exDate', 'ex date']
            for c in alias_date_cols:
                if c in df.columns and 'date' not in df.columns:
                    df['date'] = df[c]
                    break

            if not df.empty and 'date' in df.columns:
                try:
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                except Exception as err:
                    print(f"⚠️  [Validator] Failed to coerce 'date' column: {err}")

            # Client-side fallback filtering to ensure start/end are respected even if server-side filter didn't apply
            try:
                if date_filter_column and (start_date or end_date) and not df.empty:
                    # Choose the column in dataframe to filter on
                    col = date_filter_column if date_filter_column in df.columns else ('date' if 'date' in df.columns else None)
                    if col:
                        if col != 'date' and not pd.api.types.is_datetime64_any_dtype(df[col]):
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                        start_dt = pd.to_datetime(start_date) if start_date else None
                        end_dt = pd.to_datetime(end_date) if end_date else None
                        if start_dt is not None:
                            df = df[df[col] >= start_dt]
                        if end_dt is not None:
                            # For timestamps, include the whole end day by adding 1 day and using < next_day
                            if date_filter_column == 'timestamp':
                                df = df[df[col] < (end_dt + pd.Timedelta(days=1))]
                            else:
                                df = df[df[col] <= end_dt]
            except Exception as err:
                print(f"⚠️  [Validator] Failed to apply client-side date filter for {table_name}: {err}")
            
            # Return DataFrame and the applied start/end so caller can reflect actual filter used
            return df, start_date, end_date
        except Exception as e:
            # Return empty DataFrame if table doesn't exist or error occurs
            return pd.DataFrame(), start_date, end_date
    
    def _tolerance(self, base: pd.Series, rel: float, abs_tol: float) -> pd.Series:
        """Return tolerance per-row combining relative & absolute materiality."""
        return np.maximum(base.abs() * rel, abs_tol)

    def _to_json_serializable(self, obj):
        """Convert numpy/pandas types to JSON serializable Python types."""
        if pd.isna(obj):
            return None
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat() if hasattr(obj, 'isoformat') else str(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return obj

    async def _add_identity_anomalies(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Check core accounting identities (Metrik 1)."""
        anomalies: List[Dict[str, Any]] = []
        if df.empty:
            return anomalies
        x = df.copy()
        if 'date' in x.columns:
            try:
                x['date'] = pd.to_datetime(x['date'])
            except Exception:
                pass
        # 1. Assets = Liabilities + Equity (or Stockholders Equity fallback)
        # Exclude Islamic banks which have different accounting standards
        if 'total_assets' in x.columns and 'total_liabilities' in x.columns:
            equity_col = 'total_equity'
            if equity_col:
                # Skip rows with any null component (do NOT coerce to 0)
                subset = x[['total_assets','total_liabilities',equity_col]].copy()
                
                # Exclude Islamic banks from this validation
                islamic_banks = ['BANK.JK', 'BRIS.JK', 'BSIM.JK', 'PNBS.JK', 'BTPS.JK']
                if 'symbol' in x.columns:
                    subset = subset[~x['symbol'].isin(islamic_banks)]
                
                subset_non_null = subset.dropna(how='any')
                if not subset_non_null.empty:
                    lhs = subset_non_null['total_assets']
                    rhs = subset_non_null['total_liabilities'] + subset_non_null[equity_col]
                    tol = self._tolerance(lhs, 0.1, 1e9)
                    mask = (lhs - rhs).abs() > tol
                    for idx, r in subset_non_null[mask].iterrows():
                        diff = float(r['total_assets'] - (r['total_liabilities'] + r[equity_col]))
                        base = float(r['total_assets']) if r['total_assets'] not in (0, None, np.nan) else 1.0
                        diff_pct = abs(diff) / abs(base) * 100.0
                        # Severity grouping per requirement
                        if diff_pct > 11.0:
                            severity = 'error'
                        elif diff_pct > 5.0:
                            severity = 'warning'
                        else:
                            severity = 'info'
                        anomalies.append({
                            "type": "identity_violation",
                            "metric": "assets=liabilities+equity",
                            "message": "Assets do not equal Liabilities plus Equity",
                            "symbol": x.loc[idx].get('symbol'),
                            "date": x.loc[idx].get('date').strftime('%Y-%m-%d') if isinstance(x.loc[idx].get('date'), (pd.Timestamp, datetime)) else x.loc[idx].get('date'),
                            "difference": diff,
                            "difference_pct": diff_pct,
                            "severity": severity
                        })
        # 2. Net loan = Gross loan - Allowance (allowance absolute)
        if all(c in x.columns for c in ['gross_loan', 'allowance_for_loans', 'net_loan']):
            subset = x[['gross_loan','allowance_for_loans','net_loan']].copy().dropna(how='any')
            if not subset.empty:
                expected = subset['gross_loan'] - subset['allowance_for_loans'].abs()
                tol = self._tolerance(expected, 0.02, 1e9)
                diff_series = subset['net_loan'] - expected
                mask = diff_series.abs() > tol
                for idx, diff_val in diff_series[mask].items():
                    base = abs(expected.loc[idx]) if expected.loc[idx] not in (0, None, np.nan) else 1.0
                    anomalies.append({
                        "type": "identity_violation",
                        "metric": "net_loan=gross_loan-allowance",
                        "message": "Net loan does not equal Gross loan minus Allowance",
                        "symbol": x.loc[idx].get('symbol'),
                        "date": x.loc[idx].get('date').strftime('%Y-%m-%d') if isinstance(x.loc[idx].get('date'), (pd.Timestamp, datetime)) else x.loc[idx].get('date'),
                        "difference": float(diff_val),
                        "difference_pct": (abs(diff_val) / base * 100.0) if base else None,
                        "severity": "warning"
                    })
        # 3. EBT ≈ Earnings + Tax (+ Minorities optional)
        if all(c in x.columns for c in ['earnings_before_tax', 'earnings', 'tax']):
            subset_cols = ['earnings_before_tax','earnings','tax'] + (['minorities'] if 'minorities' in x.columns else [])
            subset = x[subset_cols].dropna(how='any')
            if not subset.empty:
                ebt = subset['earnings_before_tax']
                opt = subset['earnings'] + subset['tax']
                opt2 = opt + (subset['minorities'] if 'minorities' in subset.columns else 0)
                tol = self._tolerance(ebt, 0.05, 1e9)
                mask = ((ebt - opt).abs() > tol) & ((ebt - opt2).abs() > tol)
                for idx in subset[mask].index:
                    diff1 = float(ebt.loc[idx] - opt.loc[idx])
                    base = abs(ebt.loc[idx]) if ebt.loc[idx] not in (0, None, np.nan) else 1.0
                    anomalies.append({
                        "type": "identity_violation",
                        "metric": "ebt≈earnings+tax(+minorities)",
                        "message": "EBT does not equal Earnings plus Tax (plus Minorities)",
                        "symbol": x.loc[idx].get('symbol'),
                        "date": x.loc[idx].get('date').strftime('%Y-%m-%d') if isinstance(x.loc[idx].get('date'), (pd.Timestamp, datetime)) else x.loc[idx].get('date'),
                        "difference": diff1,
                        "difference_pct": abs(diff1) / base * 100.0,
                        "severity": "warning"
                    })
        # 4. Net cash flow = CFO + CFI + CFF
        if all(c in x.columns for c in ['net_operating_cash_flow','net_investing_cash_flow','net_financing_cash_flow','net_cash_flow']):
            # Rows where any component (including target) null
            required_cols = ['net_operating_cash_flow','net_investing_cash_flow','net_financing_cash_flow','net_cash_flow']
            # First mark rows where net_cash_flow is missing but components present
            comp_present_mask = x[['net_operating_cash_flow','net_investing_cash_flow','net_financing_cash_flow']].notna().all(axis=1)
            ncf_missing_mask = x['net_cash_flow'].isna() & comp_present_mask
            for idx in x[ncf_missing_mask].index:
                anomalies.append({
                    "type": "data_missing",
                    "metric": "net_cash_flow",
                    "message": "Net cash_flow value missing while components present (skipped identity check)",
                    "symbol": x.loc[idx].get('symbol'),
                    "date": x.loc[idx].get('date').strftime('%Y-%m-%d') if isinstance(x.loc[idx].get('date'), (pd.Timestamp, datetime)) else x.loc[idx].get('date'),
                    "severity": "info"
                })
            # Evaluate only fully non-null rows
            subset = x[required_cols].dropna(how='any')
            if not subset.empty:
                expected = subset['net_operating_cash_flow'] + subset['net_investing_cash_flow'] + subset['net_financing_cash_flow']
                ncf = subset['net_cash_flow']
                tol = self._tolerance(expected, 0.05, 1e9)
                diff_series = ncf - expected
                mask = diff_series.abs() > tol
                for idx, diff_val in diff_series[mask].items():
                    base = abs(expected.loc[idx]) if expected.loc[idx] not in (0, None, np.nan) else 1.0
                    anomalies.append({
                        "type": "identity_violation",
                        "metric": "net_cash_flow=sum(CFO,CFI,CFF)",
                        "message": "Net cash flow does not equal the sum of CFO, CFI, and CFF",
                        "symbol": x.loc[idx].get('symbol'),
                        "date": x.loc[idx].get('date').strftime('%Y-%m-%d') if isinstance(x.loc[idx].get('date'), (pd.Timestamp, datetime)) else x.loc[idx].get('date'),
                        "difference": float(diff_val),
                        "difference_pct": (abs(diff_val) / base * 100.0) if base else None,
                        "severity": "warning"
                    })
        # 5. Free cash flow = CFO - Capex (skip if sub_sector_id==19)
        if all(c in x.columns for c in ['free_cash_flow','net_operating_cash_flow','capital_expenditure','symbol']):
            for _, r in x.iterrows():
                symbol = r.get('symbol')
                try:
                    result = await self._get_company_data(symbol)
                    if hasattr(result, 'empty') and not result.empty:
                        sub_sector_id = int(result.iloc[0].get('sub_sector_id', None))
                    if sub_sector_id == 19:
                        continue  # skip FCF check for sub_sector_id 19
                    expected = r['net_operating_cash_flow'] - r['capital_expenditure']
                    fcf = r['free_cash_flow']
                    tol = max(abs(expected) * 0.05, 5e8)
                    if abs(fcf - expected) > tol:
                        anomalies.append({
                            "type": "identity_violation",
                            "metric": "free_cash_flow=CFO-capex",
                            "message": "Free cash flow does not equal CFO minus Capex",
                            "symbol": symbol,
                            "date": r.get('date').strftime('%Y-%m-%d') if isinstance(r.get('date'), (pd.Timestamp, datetime)) else r.get('date'),
                            "severity": "info"
                        })
                except Exception:
                    pass
        
        # 6. Total revenue ≈ net_interest_income + non_interest_income (DISABLED - high false positive)
        if 'total_revenue' in x.columns and ('net_interest_income' in x.columns or 'non_interest_income' in x.columns):
            comp = x.get('net_interest_income', pd.Series([0]*len(x))).fillna(0) + x.get('non_interest_income', pd.Series([0]*len(x))).fillna(0)
            total_rev = x['total_revenue'].fillna(0)
            # Only check if components are material (>10% of total revenue)
            material_mask = comp.abs() > (total_rev.abs() * 0.1)
            tol = self._tolerance(total_rev, 0.25, 1e9)  # Increased tolerance to 25%
            mask = material_mask & ((total_rev - comp).abs() > tol)
            for _, r in x[mask].iterrows():
                anomalies.append({
                    "type": "identity_violation",
                    "metric": "total_revenue≈net_interest+non_interest",
                    "message": "Total revenue does not equal the sum of Net Interest Income and Non-Interest Income",
                    "symbol": r.get('symbol'),
                    "date": r.get('date').strftime('%Y-%m-%d') if isinstance(r.get('date'), (pd.Timestamp, datetime)) else r.get('date'),
                    "severity": "info"
                })
        # 7. Deposits composition
        if all(c in x.columns for c in ['total_deposit','current_account','savings_account','time_deposit']):
            comp = x['current_account'].fillna(0) + x['savings_account'].fillna(0) + x['time_deposit'].fillna(0)
            total_dep = x['total_deposit'].fillna(0)
            tol = self._tolerance(total_dep, 0.03, 1e9)
            mask = (total_dep - comp).abs() > tol
            for _, r in x[mask].iterrows():
                anomalies.append({
                    "type": "identity_violation",
                    "metric": "total_deposit=components",
                    "message": "Total deposit does not equal the sum of Current Account, Savings Account, and Time Deposit",
                    "symbol": r.get('symbol'),
                    "date": r.get('date').strftime('%Y-%m-%d') if isinstance(r.get('date'), (pd.Timestamp, datetime)) else r.get('date'),
                    "severity": "info"
                })
        print(f"Found {len(anomalies)} identity anomalies")
        return anomalies

    def _add_ratio_anomalies(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Check core banking ratios (Metrik 2)."""
        anomalies: List[Dict[str, Any]] = []
        if df.empty:
            return anomalies
        x = df.copy()
        if 'date' in x.columns:
            try:
                x['date'] = pd.to_datetime(x['date'])
            except Exception:
                pass
        for _, r in x.iterrows():
            sym = r.get('symbol')
            date_val = r.get('date')
            date_str = date_val.strftime('%Y-%m-%d') if isinstance(date_val, (pd.Timestamp, datetime)) else date_val
            # Skip Islamic banks for LDR validation
            islamic_banks = ['BANK.JK', 'BRIS.JK', 'BSIM.JK', 'PNBS.JK', 'BTPS.JK']
            if sym in islamic_banks:
                continue
                
            # LDR
            if r.get('gross_loan') not in (None, np.nan) and r.get('total_deposit') not in (None, 0, np.nan):
                try:
                    ldr = r['gross_loan'] / r['total_deposit'] if r['total_deposit'] else None
                except Exception:
                    ldr = None
                if ldr is not None and (ldr < 0.4 or ldr > 1.3):
                    anomalies.append({
                        "type": "ratio_out_of_range",
                        "metric": "ldr",
                        "message": "LDR does not equal Gross Loan divided by Total Deposit (ldr < 0.4 or ldr > 1.3)",
                        "symbol": sym,
                        "date": date_str,
                        "value": float(ldr),
                        "severity": "warning"
                    })
            # CASA
            if all(k in r.index for k in ['current_account','savings_account','time_deposit']):
                total_dep_parts = sum([v for v in [r.get('current_account'), r.get('savings_account'), r.get('time_deposit')] if pd.notna(v)])
                if total_dep_parts and pd.notna(r.get('current_account')) and pd.notna(r.get('savings_account')):
                    casa = (r['current_account'] + r['savings_account']) / total_dep_parts if total_dep_parts else None
                    if casa is not None and (casa < 0 or casa > 1):
                        anomalies.append({
                            "type": "ratio_out_of_range",
                            "metric": "casa",
                            "message": "CASA does not equal the sum of Current Account and Savings Account divided by Total Deposit",
                            "symbol": sym,
                            "date": date_str,
                            "value": float(casa),
                            "severity": "warning"
                        })
            # CAR
            if 'total_capital' in r.index and 'total_risk_weighted_asset' in r.index and r.get('total_risk_weighted_asset') not in (None,0,np.nan):
                try:
                    car = r['total_capital'] / r['total_risk_weighted_asset'] if r['total_risk_weighted_asset'] else None
                except Exception:
                    car = None
                if car is not None and car < 0.1:
                    anomalies.append({
                        "type": "ratio_out_of_range",
                        "metric": "car",
                        "message": "CAR does not equal Total Capital divided by Total Risk Weighted Asset",
                        "symbol": sym,
                        "date": date_str,
                        "value": float(car),
                        "severity": "warning"
                    })
            # NIM proxy
            if r.get('net_interest_income') not in (None, np.nan) and r.get('total_assets') not in (None, 0, np.nan):
                try:
                    nim_proxy = r['net_interest_income'] / r['total_assets'] if r['total_assets'] else None
                except Exception:
                    nim_proxy = None
                # Adjusted range: -2% to 25% (more realistic for Indonesian banks in volatile conditions)
                if nim_proxy is not None and (nim_proxy < -0.02 or nim_proxy > 0.25):
                    anomalies.append({
                        "type": "ratio_out_of_range",
                        "metric": "nim_proxy",
                        "message": f"NIM proxy {nim_proxy*100:.2f}% is outside reasonable range (-2% to 25%)",
                        "symbol": sym,
                        "date": date_str,
                        "value": float(nim_proxy),
                        "severity": "info"
                    })
            # Cost to income
            if r.get('operating_expense') not in (None, np.nan):
                income_components = 0.0
                for k in ['net_interest_income','non_interest_income']:
                    if k in r.index and pd.notna(r.get(k)):
                        income_components += r.get(k)
                if income_components:
                    try:
                        cir = r['operating_expense'] / income_components if income_components else None
                    except Exception:
                        cir = None
                    # Only flag extreme cases: <0% or >300% (digital banks can have high CIR initially)
                    if cir is not None and (cir < 0 or cir > 3.0):
                        anomalies.append({
                            "type": "ratio_out_of_range",
                            "metric": "cost_to_income",
                            "message": f"Cost to Income Ratio {cir*100:.1f}% is extremely high (>300%) or negative",
                            "symbol": sym,
                            "date": date_str,
                            "value": float(cir),
                            "severity": "warning"
                        })
            # Coverage ratio
            if r.get('allowance_for_loans') not in (None, np.nan) and r.get('gross_loan') not in (None, 0, np.nan):
                try:
                    coverage = abs(r['allowance_for_loans']) / r['gross_loan'] if r['gross_loan'] else None
                except Exception:
                    coverage = None
                # Only flag extreme cases: >50% (very conservative) or negative
                if coverage is not None and (coverage < 0 or coverage > 0.5):
                    anomalies.append({
                        "type": "ratio_out_of_range",
                        "metric": "coverage_ratio",
                        "message": f"Coverage ratio {coverage*100:.1f}% is extremely high (>50%) indicating over-provisioning",
                        "symbol": sym,
                        "date": date_str,
                        "value": float(coverage),
                        "severity": "info"
                    })
        return anomalies
    
    async def _validate_financial_annual(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate idx_combine_financials_annual table
        Condition: absolute change per annual > 50%, but check average changes per period
        """
        anomalies = []
        
        try:
            # Ensure we have required columns
            required_cols = ['date', 'symbol', 'revenue', 'earnings', 'total_assets']
            missing_cols = [col for col in required_cols if col not in data.columns]
            
            if missing_cols:
                anomalies.append({
                    "type": "missing_required_columns",
                    "columns": missing_cols,
                    "message": f"Missing required columns: {', '.join(missing_cols)}",
                    "severity": "error"
                })
                return {"anomalies": anomalies}
            
            # Convert date to datetime and extract year
            data = data.copy()
            data['date'] = pd.to_datetime(data['date'])
            data['year'] = data['date'].dt.year
            
            # Group by symbol and analyze year-over-year changes
            financial_metrics = ['revenue', 'earnings', 'total_assets']
            available_metrics = [col for col in financial_metrics if col in data.columns]
            
            for symbol in data['symbol'].unique():
                symbol_data = data[data['symbol'] == symbol].sort_values('year')
                
                if len(symbol_data) < 2:
                    continue  # Need at least 2 years of data
                
                for metric in available_metrics:
                    if metric not in symbol_data.columns:
                        continue
                    
                    # Skip if all values are null/NaN for this metric
                    if symbol_data[metric].isna().all():
                        continue
                    
                    # Calculate year-over-year percentage changes
                    symbol_data = symbol_data.copy()
                    symbol_data[f'{metric}_pct_change'] = symbol_data[metric].pct_change(fill_method=None) * 100
                    
                    # Get changes excluding first row (which will be NaN)
                    changes = symbol_data[f'{metric}_pct_change'].dropna()
                    
                    if len(changes) == 0:
                        continue
                    
                    # MORE STRINGENT: Calculate average absolute change and use higher threshold
                    avg_abs_change = changes.abs().mean()
                    
                    # Only flag if:
                    # 1. Change > 75% (increased from 50%)
                    # 2. Change > 2x average (increased from 1.5x)
                    # 3. At least 2 extreme changes to avoid one-off events
                    extreme_pct_changes = changes[(changes.abs() > 75) & (changes.abs() > (avg_abs_change * 2.0))]
                    
                    # Only trigger if multiple extreme changes (more than 1)
                    if len(extreme_pct_changes) > 1:
                        years_affected = symbol_data[symbol_data[f'{metric}_pct_change'].abs() > 75]['year'].tolist()
                        
                        anomalies.append({
                            "type": "extreme_annual_change",
                            "symbol": symbol,
                            "metric": metric,
                            "years_affected": years_affected,
                            "extreme_pct_changes": extreme_pct_changes.tolist(),
                            "avg_abs_change": round(avg_abs_change, 2),
                            "message": f"Symbol {symbol}: {metric} shows multiple extreme annual changes (>75%) in years {years_affected}. Average absolute change: {avg_abs_change:.1f}%",
                            "severity": "error"
                        })
            # Revenue should be greater than earnings
            if all(col in data.columns for col in ['revenue', 'earnings']):
                rev = pd.to_numeric(data['revenue'], errors='coerce')
                earn = pd.to_numeric(data['earnings'], errors='coerce')
                mask = rev.notna() & earn.notna() & (rev <= earn)
                for idx in data[mask].index:
                    date_val = data.loc[idx].get('date')
                    date_str = date_val.strftime('%Y-%m-%d') if isinstance(date_val, (pd.Timestamp, datetime)) else date_val
                    anomalies.append({
                        "type": "business_rule_violation",
                        "metric": "revenue>earnings",
                        "message": "Revenue should be greater than Earnings (annual)",
                        "symbol": data.loc[idx].get('symbol'),
                        "date": date_str,
                        "revenue": float(rev.loc[idx]) if pd.notna(rev.loc[idx]) else None,
                        "earnings": float(earn.loc[idx]) if pd.notna(earn.loc[idx]) else None,
                        "severity": "error"
                    })
            # Tambah Metrik 1 & 2 (hanya yang akurat)
            # Only run identity/ratio checks if we have sufficient data volume
            if len(data) > 10:  # Avoid ratio checks on small datasets
                critical_identities = await self._add_identity_anomalies(data)
                critical_ratios = self._add_ratio_anomalies(data)
                anomalies.extend(critical_identities)
                anomalies.extend(critical_ratios)
                    
            # if len(anomalies) > 1:
            #     for anomaly in anomalies:
            #         print(anomaly['message'])
        
        except Exception as e:
            anomalies.append({
                "type": "validation_error",
                "message": f"Error validating annual financial data: {str(e)}",
                "severity": "error"
            })
        
        return {"anomalies": anomalies}

    async def _validate_index_daily_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate index_daily_data table
        For each date, there must be exactly 18 index_code entries.
        (default last month if no start/end provided).
        """
        anomalies: List[Dict[str, Any]] = []
        try:
            if data is None or data.empty:
                return {"anomalies": anomalies}

            x = data.copy()
            # Schema checks before using columns
            if 'date' not in x.columns:
                anomalies.append({
                    "severity": "error",
                    "type": "schema_missing_column",
                    "message": "Column 'date' is missing in index_daily_data",
                    "details": {"required_column": "date"}
                })
                return {"anomalies": anomalies}

            if 'index_code' not in x.columns:
                anomalies.append({
                    "severity": "error",
                    "type": "schema_missing_column",
                    "message": "Column 'index_code' is missing in index_daily_data",
                    "details": {"required_column": "index_code"}
                })
                return {"anomalies": anomalies}

            # Parse date to datetime
            x['date'] = pd.to_datetime(x['date'], errors='coerce')
            # Normalize to yyyy-mm-dd string for grouping/reporting
            x['date'] = x['date'].dt.strftime('%Y-%m-%d')

            # Count not null index_code per date
            counts = (
                x.groupby('date')['index_code']
                 .count()
                 .reset_index(name='index_count')
            )
            EXPECTED_COUNT = 18
            for _, row in counts.iterrows():
                date_val = row['date']
                cnt = int(row['index_count'])
                if cnt != EXPECTED_COUNT:
                    codes = (
                        x.loc[x['date'] == date_val, 'index_code']
                         .dropna()
                         .astype(str)
                         .tolist()
                    )
                    anomalies.append({
                        "severity": "error",
                        "type": "index_daily_data_count_mismatch",
                        "message": f"Expected {EXPECTED_COUNT} non-null index_code entries on {date_val}, found {cnt}",
                        "date": str(date_val),
                        "details": {
                            "found_count": cnt,
                            "expected_count": EXPECTED_COUNT,
                            "present_index_codes_sample": codes[:25]
                        }
                    })
        
        except Exception as e:
            anomalies.append({
                "severity": "error",
                "type": "validation_exception",
                "message": f"index_daily_data validation failed: {str(e)}",
                "details": {}
            })

        return {"anomalies": anomalies}
    
    async def _validate_financial_quarterly(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate idx_combine_financials_quarterly table
        Condition: absolute change per quarter > 50%, but check average changes per period
        """
        anomalies = []
        try:
            # Ensure we have required columns
            required_cols = ['date', 'symbol', 'total_revenue', 'earnings', 'total_assets']
            missing_cols = [col for col in required_cols if col not in data.columns]
            if missing_cols:
                anomalies.append({
                    "type": "missing_required_columns",
                    "columns": missing_cols,
                    "message": f"Missing required columns: {', '.join(missing_cols)}",
                    "severity": "error"
                })
                return {"anomalies": anomalies}
            
            # Create period identifier
            data = data.copy()
            data['date'] = pd.to_datetime(data['date'])

            # Group by symbol and analyze year-over-year changes
            financial_metrics = ['total_revenue', 'earnings', 'total_assets']
            available_metrics = [col for col in financial_metrics if col in data.columns]

            for symbol in data['symbol'].unique():
                symbol_data = data[data['symbol'] == symbol].sort_values('date')
                if len(symbol_data) < 4:
                    continue  # Need at least 4 quarters of data
                for metric in available_metrics:
                    if metric not in symbol_data.columns:
                        continue
                    if symbol_data[metric].isna().all():
                        continue
                    symbol_data = symbol_data.copy()
                    symbol_data[f'{metric}_pct_change'] = symbol_data[metric].pct_change(fill_method=None) * 100
                    changes = symbol_data[f'{metric}_pct_change'].dropna()
                    if len(changes) == 0:
                        continue
                    avg_abs_change = changes.abs().mean()
                    
                    # MORE STRINGENT for quarterly: 
                    # 1. Change > 100% (doubled from 50% - quarterly can be more volatile)
                    # 2. Change > 2.5x average (increased from 1.5x)
                    # 3. At least 2 extreme changes to avoid seasonal/one-off events
                    extreme_pct_changes = changes[(changes.abs() > 100) & (changes.abs() > (avg_abs_change * 2.5))]
                    
                    # Only trigger if multiple extreme changes
                    if len(extreme_pct_changes) > 1:
                        periods_affected = symbol_data[symbol_data[f'{metric}_pct_change'].abs() > 100]['date'].dt.strftime('%Y-%m-%d').tolist()
                        anomalies.append({
                            "type": "extreme_quarterly_change",
                            "symbol": symbol,
                            "metric": metric,
                            "periods_affected": periods_affected,
                            "extreme_pct_changes": extreme_pct_changes.tolist(),
                            "avg_abs_change": round(avg_abs_change, 2),
                            "message": f"Symbol {symbol}: {metric} shows multiple extreme quarterly changes (>100%) in periods {periods_affected}. Average absolute change: {avg_abs_change:.1f}%",
                            "severity": "error"
                        })
            # total_revenue should be greater than earnings
            if all(col in data.columns for col in ['total_revenue', 'earnings']):
                trev = pd.to_numeric(data['total_revenue'], errors='coerce')
                earn = pd.to_numeric(data['earnings'], errors='coerce')
                mask = trev.notna() & earn.notna() & (trev <= earn)
                for idx in data[mask].index:
                    date_val = data.loc[idx].get('date')
                    date_str = date_val.strftime('%Y-%m-%d') if isinstance(date_val, (pd.Timestamp, datetime)) else date_val
                    anomalies.append({
                        "type": "business_rule_violation",
                        "metric": "revenue>earnings",
                        "message": "Total Revenue should be greater than Earnings (quarterly)",
                        "symbol": data.loc[idx].get('symbol'),
                        "date": date_str,
                        "total_revenue": float(trev.loc[idx]) if pd.notna(trev.loc[idx]) else None,
                        "earnings": float(earn.loc[idx]) if pd.notna(earn.loc[idx]) else None,
                        "severity": "error"
                    })
            # Only run identity/ratio checks if we have sufficient data volume
            if len(data) > 10:  # Avoid ratio checks on small datasets
                critical_identities = await self._add_identity_anomalies(data)
                critical_ratios = self._add_ratio_anomalies(data)
                anomalies.extend(critical_identities)
                anomalies.extend(critical_ratios)
                    
            # if len(anomalies) > 1:
            #     for anomaly in anomalies:
            #         print(anomaly['message'])
        except Exception as e:
            anomalies.append({
                "type": "validation_error",
                "message": f"Error validating quarterly financial data: {str(e)}",
                "severity": "error"
            })
        return {"anomalies": anomalies}
    
    async def _validate_daily_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate idx_daily_data table
        Condition: close price change > 35%
        Count is evaluated on the data window returned from _fetch_table_data_with_filter
        (default last 7 days if no start/end provided)
        """
        anomalies = []
        try:
            # Ensure we have required columns
            required_cols = ['date', 'symbol', 'close']
            missing_cols = [col for col in required_cols if col not in data.columns]
            if missing_cols:
                anomalies.append({
                    "type": "missing_required_columns",
                    "columns": missing_cols,
                    "message": f"Missing required columns: {', '.join(missing_cols)}",
                    "severity": "error"
                })
                return {"anomalies": anomalies}

            data = data.copy()
            data['date'] = pd.to_datetime(data['date'], errors='coerce')

            for symbol in data['symbol'].unique():
                symbol_data = data[data['symbol'] == symbol].sort_values('date')
                if len(symbol_data) < 2:
                    continue  # Need at least 2 days of data
                # Calculate daily price changes
                symbol_data = symbol_data.copy()
                symbol_data['price_pct_change'] = symbol_data['close'].pct_change() * 100
                # Find days with >35% price change
                extreme_pct_changes = symbol_data[symbol_data['price_pct_change'].abs() > 35]
                if len(extreme_pct_changes) > 0:
                    for _, row in extreme_pct_changes.iterrows():
                        anomalies.append({
                            "type": "extreme_daily_price_change",
                            "symbol": symbol,
                            "date": row['date'].strftime('%Y-%m-%d'),
                            "close_price": float(row['close']),
                            "price_change_pct": round(float(row['price_pct_change']), 2),
                            "message": f"Symbol {symbol} on {row['date'].strftime('%Y-%m-%d')}: Extreme daily price change detected",
                            "severity": "warning"
                        })

            # # Run additional completeness and coverage checks as a separate rule set
            # try:
            #     extra = await self._validate_daily_data_completeness_and_coverage(data)
            #     anomalies.extend(extra.get("anomalies", []))
            # except Exception as inner_err:
            #     anomalies.append({
            #         "type": "validation_error",
            #         "message": f"Error running completeness/coverage checks: {str(inner_err)}",
            #         "severity": "error"
            #     })
        except Exception as e:
            anomalies.append({
                "type": "validation_error",
                "message": f"Error validating daily data: {str(e)}",
                "severity": "error"
            })
        return {"anomalies": anomalies}

    async def _validate_daily_data_completeness_and_coverage(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Additional rules for idx_daily_data:
        1) For each date, the number of stocks (distinct symbols) must equal the number of symbols
           found in idx_active_company_profile.
        2) For each date, all of close, volume, and market_cap must be non-null for every symbol.

        Notes:
        - Uses the same filtered window that the caller provided, but narrows to "yesterday (weekday)" only.
        - Symbols are compared case-sensitively as stored; we coerce to string and strip whitespace for robustness.
        """
        anomalies: List[Dict[str, Any]] = []
        try:
            if data is None or data.empty:
                return {"anomalies": anomalies}

            x = data.copy()
            # Required columns for these checks
            required_cols = ['date', 'symbol', 'close', 'volume', 'market_cap']
            missing_cols = [c for c in required_cols if c not in x.columns]
            if missing_cols:
                anomalies.append({
                    "severity": "error",
                    "type": "missing_required_columns",
                    "message": f"Missing required columns for completeness/coverage: {', '.join(missing_cols)}",
                    "columns": missing_cols
                })
                return {"anomalies": anomalies}

            # Normalize types
            x['date'] = pd.to_datetime(x['date'], errors='coerce')
            x['symbol'] = x['symbol'].astype(str).str.strip()

            # Iterate over dates present in the data
            if x.empty:
                return {"anomalies": anomalies}

            # Fetch active symbols from idx_active_company_profile
            try:
                resp = self.supabase.table('idx_active_company_profile').select('symbol').execute()
                active_rows = getattr(resp, 'data', None) or []
                active_df = pd.DataFrame(active_rows)
                if 'symbol' not in active_df.columns or active_df.empty:
                    active_symbols: List[str] = []
                else:
                    active_symbols = (
                        active_df['symbol'].astype(str).str.strip().dropna().unique().tolist()
                    )
            except Exception as fetch_err:
                # If we cannot fetch active list, record an error and skip coverage check
                anomalies.append({
                    "severity": "error",
                    "type": "reference_table_fetch_error",
                    "message": f"Failed to fetch active symbols from idx_active_company_profile: {str(fetch_err)}"
                })
                active_symbols = []

            # Build case-insensitive active symbol set and a mapper to original casing
            def _norm_sym(s: Any) -> str:
                try:
                    return str(s).strip().upper()
                except Exception:
                    return ""

            active_original_list: List[str] = active_symbols
            active_upper_set = { _norm_sym(s) for s in active_original_list if _norm_sym(s) }
            active_upper_to_original: Dict[str, str] = {}
            for s in active_original_list:
                su = _norm_sym(s)
                if su and su not in active_upper_to_original:
                    active_upper_to_original[su] = s
            active_count = len(active_upper_set)

            # Prepare per-day checks
            if x['date'].isna().all():
                anomalies.append({
                    "severity": "error",
                    "type": "invalid_date_values",
                    "message": "All 'date' values could not be parsed to datetime in idx_daily_data"
                })
                return {"anomalies": anomalies}

            x['date_only'] = x['date'].dt.strftime('%Y-%m-%d')

            for date_val, day_df in x.groupby('date_only'):
                # 1) Coverage: compare sets of symbols (unique, case-insensitive)
                day_original_list = (
                    day_df['symbol'].dropna().astype(str).str.strip().tolist()
                )
                day_upper_set = { _norm_sym(s) for s in day_original_list if _norm_sym(s) }
                day_upper_to_original: Dict[str, str] = {}
                for s in day_original_list:
                    su = _norm_sym(s)
                    if su and su not in day_upper_to_original:
                        day_upper_to_original[su] = s

                if active_count > 0 and day_upper_set != active_upper_set:
                    missing_upper = sorted(list(active_upper_set - day_upper_set))
                    unexpected_upper = sorted(list(day_upper_set - active_upper_set))

                    missing_symbols_original = [active_upper_to_original[u] for u in missing_upper][:50]
                    unexpected_symbols_original = [day_upper_to_original[u] for u in unexpected_upper][:50]

                    # Primary message focuses on missing symbols per request
                    if missing_symbols_original:
                        msg = f"On {date_val}, {', '.join(missing_symbols_original)} not present in idx_daily_data"
                    else:
                        msg = f"On {date_val}, unexpected symbols present: {', '.join(unexpected_symbols_original)}"

                    anomalies.append({
                        "severity": "error",
                        "type": "daily_symbol_coverage_mismatch",
                        "message": msg,
                        "date": date_val,
                        "details": {
                            "found_count": len(day_upper_set),
                            "expected_count": active_count,
                            "missing_symbols_sample": missing_symbols_original,
                            "unexpected_symbols_sample": unexpected_symbols_original
                        }
                    })

                # 2) Non-null checks for close, volume, market_cap
                null_close = day_df[day_df['close'].isna()]
                null_volume = day_df[day_df['volume'].isna()]
                null_mcap = day_df[day_df['market_cap'].isna()]

                if not null_close.empty or not null_volume.empty or not null_mcap.empty:
                    # Build a concise message listing a few affected symbols per field
                    def _syms(df):
                        return df['symbol'].dropna().astype(str).str.strip().unique().tolist()

                    close_syms = _syms(null_close)[:10]
                    volume_syms = _syms(null_volume)[:10]
                    mcap_syms = _syms(null_mcap)[:10]

                    parts = []
                    if close_syms:
                        parts.append(f"close: {', '.join(close_syms)}")
                    if volume_syms:
                        parts.append(f"volume: {', '.join(volume_syms)}")
                    if mcap_syms:
                        parts.append(f"market_cap: {', '.join(mcap_syms)}")
                    summary = "; ".join(parts) if parts else ""

                    anomalies.append({
                        "severity": "error",
                        "type": "daily_required_fields_null",
                        "message": f"On {date_val}, some required fields are null — {summary}",
                        "date": date_val,
                        "details": {
                            "null_close_symbols_sample": _syms(null_close)[:50],
                            "null_volume_symbols_sample": _syms(null_volume)[:50],
                            "null_market_cap_symbols_sample": _syms(null_mcap)[:50],
                            "null_close_count": int(null_close.shape[0]),
                            "null_volume_count": int(null_volume.shape[0]),
                            "null_market_cap_count": int(null_mcap.shape[0])
                        }
                    })

        except Exception as e:
            anomalies.append({
                "severity": "error",
                "type": "validation_exception",
                "message": f"idx_daily_data completeness/coverage validation failed: {str(e)}",
                "details": {}
            })

        return {"anomalies": anomalies}

    async def _validate_sgx_company_report(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate sgx_company_report table (Top 50 companies by market cap)
        Rules:
        1) market_cap and volume must be non-null
        2) close: latest available date equals today's date
        3) historical_financials: percent change per period checks
        """
        anomalies: List[Dict[str, Any]] = []
        try:
            x = data.copy()
            print(f"🏢 [SGX Validator] Validating {len(x)} companies (top 50 by market cap)")
            
            # Normalize dates if present
            if 'date' in x.columns:
                try:
                    x['date'] = pd.to_datetime(x['date'], errors='coerce')
                except Exception:
                    pass

            # 1) market_cap and volume not null
            for col in ['market_cap', 'volume']:
                if col not in x.columns:
                    anomalies.append({
                        "type": "missing_required_columns",
                        "columns": [col],
                        "message": f"Missing required column: {col}",
                        "severity": "error"
                    })
                else:
                    null_mask = x[col].isna()
                    for idx in x[null_mask].index:
                        date_val = x.loc[idx].get('date')
                        date_str = date_val.strftime('%Y-%m-%d') if isinstance(date_val, (pd.Timestamp, datetime)) else date_val
                        anomalies.append({
                            "type": "data_missing",
                            "metric": col,
                            "message": f"{col} is null",
                            "symbol": x.loc[idx].get('symbol'),
                            "date": date_str,
                            "severity": "error"
                        })

            # 2) close latest date equals today's UTC date
            if 'close' not in x.columns:
                anomalies.append({
                    "type": "missing_required_columns",
                    "columns": ["close"],
                    "message": "Missing required column: close",
                    "severity": "error"
                })
            else:
                # Use Singapore date (UTC+8)
                try:
                    sg_tz = pytz.timezone('Asia/Singapore')
                    today_sg = datetime.now(sg_tz).date()
                except Exception:
                    # Fallback: system local date if timezone not available
                    today_sg = datetime.now().date()
                for idx, r in x.iterrows():
                    sym = r.get('symbol')
                    close_obj = r.get('close')
                    latest_date = None
                    try:
                        # close may be a dict of date->price
                        if isinstance(close_obj, dict):
                            if len(close_obj) > 0:
                                # keys as dates
                                date_keys = []
                                for k in close_obj.keys():
                                    try:
                                        date_keys.append(pd.to_datetime(k, errors='coerce'))
                                    except Exception:
                                        date_keys.append(pd.NaT)
                                date_keys = [d for d in date_keys if pd.notna(d)]
                                if date_keys:
                                    latest_date = max(date_keys).date()
                        # alternatively list of dicts or list of [date, value]
                        elif isinstance(close_obj, list) and len(close_obj) > 0:
                            # try to parse items
                            parsed_dates = []
                            for item in close_obj:
                                if isinstance(item, dict):
                                    # common keys
                                    for key in ['date', 'Date', 'timestamp']:
                                        if key in item:
                                            parsed_dates.append(pd.to_datetime(item[key], errors='coerce'))
                                            break
                                elif isinstance(item, (list, tuple)) and len(item) >= 1:
                                    parsed_dates.append(pd.to_datetime(item[0], errors='coerce'))
                            parsed_dates = [d for d in parsed_dates if pd.notna(d)]
                            if parsed_dates:
                                latest_date = max(parsed_dates).date()
                    except Exception:
                        latest_date = None

                    if latest_date is None:
                        anomalies.append({
                            "type": "data_missing",
                            "metric": "close.latest_date",
                            "message": "Unable to determine latest close date",
                            "symbol": sym,
                            "severity": "error"
                        })
                    else:
                        if latest_date != today_sg:
                            anomalies.append({
                                "type": "staleness",
                                "metric": "close.latest_date",
                                "message": f"Latest close date {latest_date} does not equal today's Singapore date {today_sg}",
                                "symbol": sym,
                                "latest_close_date": latest_date.isoformat(),
                                "today_sg": today_sg.isoformat(),
                                "severity": "warning"
                            })

            # 3) historical_financials percent-change checks
            if 'historical_financials' not in x.columns:
                anomalies.append({
                    "type": "missing_required_columns",
                    "columns": ["historical_financials"],
                    "message": "Missing required column: historical_financials",
                    "severity": "warning"
                })
            else:
                metrics = ['revenue', 'earnings', 'total_assets', 'total_equity', 'operating_pnl']
                for idx, r in x.iterrows():
                    sym = r.get('symbol')
                    hf = r.get('historical_financials')
                    if not isinstance(hf, (list, tuple)) or len(hf) < 2:
                        continue
                    # Build DataFrame
                    try:
                        hdf = pd.DataFrame(hf)
                    except Exception:
                        continue
                    # Determine time axis
                    sort_col = None
                    for c in ['date', 'Date', 'period', 'year', 'Year']:
                        if c in hdf.columns:
                            sort_col = c
                            break
                    if sort_col is None:
                        continue
                    # Parse to datetime if looks like date
                    if sort_col.lower() in ['date', 'timestamp']:
                        hdf[sort_col] = pd.to_datetime(hdf[sort_col], errors='coerce')
                    hdf = hdf.sort_values(sort_col)
                    # Compute percent changes similar to quarterly (stricter)
                    for m in [m for m in metrics if m in hdf.columns]:
                        series = pd.to_numeric(hdf[m], errors='coerce')
                        pct = series.pct_change(fill_method=None) * 100
                        changes = pct.dropna()
                        if changes.empty:
                            continue
                        avg_abs_change = changes.abs().mean()
                        extreme = changes[(changes.abs() > 100) & (changes.abs() > (avg_abs_change * 2.5))]
                        if len(extreme) > 1:
                            # derive periods affected for messaging
                            periods = hdf.loc[pct.abs() > 100, sort_col].tolist()
                            periods = [p.strftime('%Y-%m-%d') if isinstance(p, (pd.Timestamp, datetime)) else str(p) for p in periods]
                            anomalies.append({
                                "type": "extreme_change_sgx",
                                "metric": m,
                                "symbol": sym,
                                "periods_affected": periods,
                                "extreme_pct_changes": extreme.tolist(),
                                "avg_abs_change": round(float(avg_abs_change), 2),
                                "message": f"{sym}: {m} shows multiple extreme period changes (>100%) in {periods}. Avg abs change: {avg_abs_change:.1f}%",
                                "severity": "warning"
                            })

        except Exception as e:
            anomalies.append({
                "type": "validation_error",
                "message": f"Error validating sgx_company_report: {str(e)}",
                "severity": "error"
            })

        return {"anomalies": anomalies}
    
    async def _validate_dividend(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate idx_dividend table
        Conditions:
        1. average yield per year >= 30%
        2. yield (average) per year change >= 10%
        """
        anomalies = []
        try:
            # Work on a copy and normalize common alias column names that appear in different pipelines
            data = data.copy()
            required_cols = ['symbol', 'yield', 'date']
            missing_cols = [col for col in required_cols if col not in data.columns]
            if missing_cols:
                anomalies.append({
                    "type": "missing_required_columns",
                    "columns": missing_cols,
                    "message": f"Missing required columns: {', '.join(missing_cols)}",
                    "severity": "error"
                })
                return {"anomalies": anomalies}

            # Parse dates with coercion so bad values become NaT (we will report these later)
            try:
                data['date'] = pd.to_datetime(data['date'], errors='coerce')
            except KeyError:
                anomalies.append({
                    "type": "missing_required_columns",
                    "columns": ['date'],
                    "message": "Missing required column: date",
                    "severity": "error"
                })
                return {"anomalies": anomalies}
            # data = data[~data['yield'].isna()]
            data['year'] = data['date'].dt.year

            for symbol in data['symbol'].unique():
                symbol_data = data[data['symbol'] == symbol]
                # data daily
                try:
                    daily_data = await self._fetch_ticker_data('idx_daily_data', symbol)
                except Exception as e:
                    daily_data = None
                if symbol_data.empty:
                    continue
                # yield per tahun >= 30%
                yearly_yield = symbol_data.groupby('year')['yield'].sum()
                yearly_div = symbol_data.groupby('year')['dividend'].sum()
                this_year = datetime.now().year

                # Calculate average yield for this year
                avg_close_this_year = None
                if daily_data is not None:
                    
                    # Normalize date column in daily_data too (may have different column names)
                    if 'date' not in daily_data.columns:
                        # Try common daily data date aliases
                        date_aliases = ['trading_date', 'trade_date', 'date_trading', 'timestamp']
                        for alias in date_aliases:
                            if alias in daily_data.columns:
                                daily_data['date'] = daily_data[alias]
                                break
                    
                    if 'date' in daily_data.columns:
                        daily_data['date'] = pd.to_datetime(daily_data['date'], errors='coerce')
                        daily_this_symbol = daily_data[(daily_data['symbol'] == symbol) & (daily_data['date'].dt.year == this_year)]
                        
                        if not daily_this_symbol.empty:
                            avg_close_this_year = daily_this_symbol['close'].mean()
                        # print(f"Average close price for {symbol} this year: {avg_close_this_year}")
                    else:
                        # If daily_data has no recognizable date column, skip this year calculation
                        pass

                div_this_year = yearly_div.get(this_year)
                yield_this_year = None
                if div_this_year is not None and avg_close_this_year is not None and avg_close_this_year != 0:
                    yield_this_year = div_this_year / avg_close_this_year

                if yield_this_year is not None:
                    yearly_yield.loc[this_year] = yield_this_year

                high_yield_years = yearly_yield[yearly_yield >= 0.3]
                if not high_yield_years.empty:
                    for year, avg_yield in high_yield_years.items():
                        anomalies.append({
                            "type": "high_average_yield_per_year",
                            "symbol": symbol,
                            "year": int(year),
                            "average_yield": float(avg_yield),
                            "message": f"symbol {symbol} year {year}: Average yield {avg_yield*100:.2f}% >= 30%",
                            "severity": "warning"
                        })
                yearly_yield_sorted = yearly_yield.sort_index()
                
                yearly_yield_change = yearly_yield_sorted.diff().abs()
                
                large_changes = yearly_yield_change[yearly_yield_change >= 0.1]
                if not large_changes.empty:
                    for year, change in large_changes.items():
                        anomalies.append({
                            "type": "large_average_yield_change_per_year",
                            "symbol": symbol,
                            "year": int(year),
                            "yield_change": float(change),
                            "message": f"symbol {symbol} year {year}: Yield change {change*100:.2f}% >= 20%",
                            "severity": "warning"
                        })
        except Exception as e:
            anomalies.append({
                "type": "validation_error",
                "message": f"Error validating dividend data: {str(e)}",
                "severity": "error"
            })
        return {"anomalies": anomalies}
    
    async def _validate_all_time_price(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate idx_all_time_price table
        Condition: Check if data is inline
        """
        anomalies = []
        try:
            # Ensure required columns
            required_cols = ['symbol', 'type', 'date', 'price']
            missing_cols = [col for col in required_cols if col not in data.columns]
            if missing_cols:
                anomalies.append({
                    "type": "missing_required_columns",
                    "columns": missing_cols,
                    "message": f"Missing required columns: {', '.join(missing_cols)}",
                    "severity": "error"
                })
                return {"anomalies": anomalies}

            # For each symbol, get price for each type
            data = data.copy()
            data['date'] = pd.to_datetime(data['date'])
            pivoted = data.pivot_table(index='symbol', columns='type', values='price', aggfunc='first')
            pivoted = pivoted.reset_index()

            # Map type names in JSON to logical periods
            type_map = {
                'all_time_high': 'all_time_high',
                'all_time_low': 'all_time_low',
                '52_w_high': '52w_high',
                '52_w_low': '52w_low',
                '90_d_high': '90d_high',
                '90_d_low': '90d_low',
                'ytd_high': 'ytd_high',
                'ytd_low': 'ytd_low',
            }

            for _, row in pivoted.iterrows():
                symbol = row['symbol']
                issues = []
                # Extract values for each logical period
                values = {}
                for json_type, logic_name in type_map.items():
                    if json_type in row and pd.notna(row[json_type]):
                        values[logic_name] = float(row[json_type])

                # Cross-check each recorded high/low against daily data windows
                try:
                    daily_data = await self._fetch_ticker_data('idx_daily_data', symbol)
                except Exception:
                    daily_data = None

                tol = 1e-6  # small numeric tolerance
                if daily_data is not None and not daily_data.empty:
                    dd = daily_data.copy()
                    if 'date' in dd.columns:
                        dd['date'] = pd.to_datetime(dd['date'], errors='coerce')
                    else:
                        dd['date'] = pd.NaT
                    # Ensure close column is numeric
                    if 'close' in dd.columns:
                        dd['close'] = pd.to_numeric(dd['close'], errors='coerce')
                    # Filter to this symbol if symbol column exists
                    if 'symbol' in dd.columns:
                        dd = dd[dd['symbol'] == symbol]
                    # Drop rows without required fields
                    dd = dd.dropna(subset=['date', 'close']) if {'date','close'}.issubset(dd.columns) else pd.DataFrame()

                    if not dd.empty:
                        jakarta_tz = pytz.timezone('Asia/Jakarta')
                        today = pd.Timestamp(datetime.now(jakarta_tz).date())
                        start_90d = today - pd.Timedelta(days=90)
                        start_52w = today - pd.Timedelta(weeks=52)
                        start_ytd = pd.Timestamp(today.year, 1, 1)

                        # Windowed subsets
                        dd_90d = dd[(dd['date'] >= start_90d) & (dd['date'] <= today)]
                        dd_52w = dd[(dd['date'] >= start_52w) & (dd['date'] <= today)]
                        dd_ytd = dd[(dd['date'] >= start_ytd) & (dd['date'] <= today)]

                        # Compute mins/maxes where applicable
                        def _safe_max(frame):
                            return float(frame['close'].max()) if not frame.empty else None
                        def _safe_min(frame):
                            return float(frame['close'].min()) if not frame.empty else None

                        max_all = _safe_max(dd)
                        min_all = _safe_min(dd)
                        max_90d = _safe_max(dd_90d)
                        min_90d = _safe_min(dd_90d)
                        max_52w = _safe_max(dd_52w)
                        min_52w = _safe_min(dd_52w)
                        max_ytd = _safe_max(dd_ytd)
                        min_ytd = _safe_min(dd_ytd)

                        # High checks: recorded should be >= daily max
                        if '90d_high' in values and max_90d is not None and values['90d_high'] + tol < max_90d:
                            issues.append(f"90d_high {values['90d_high']:.1f} < daily max 90d {max_90d:.1f} in {max}")
                        if '52w_high' in values and max_52w is not None and values['52w_high'] + tol < max_52w:
                            issues.append(f"52w_high {values['52w_high']:.1f} < daily max 52w {max_52w:.1f}")
                        if 'ytd_high' in values and max_ytd is not None and values['ytd_high'] + tol < max_ytd:
                            issues.append(f"ytd_high {values['ytd_high']:.1f} < daily max ytd {max_ytd:.1f}")
                        if 'all_time_high' in values and max_all is not None and values['all_time_high'] + tol < max_all:
                            issues.append(f"all_time_high {values['all_time_high']:.1f} < daily max all_time {max_all:.1f}")

                        # Low checks: recorded should be <= daily min
                        if '90d_low' in values and min_90d is not None and values['90d_low'] - tol > min_90d:
                            issues.append(f"90d_low {values['90d_low']:.1f} > daily min 90d {min_90d:.1f}")
                        if '52w_low' in values and min_52w is not None and values['52w_low'] - tol > min_52w:
                            issues.append(f"52w_low {values['52w_low']:.1f} > daily min 52w {min_52w:.1f}")
                        if 'ytd_low' in values and min_ytd is not None and values['ytd_low'] - tol > min_ytd:
                            issues.append(f"ytd_low {values['ytd_low']:.1f} > daily min ytd {min_ytd:.1f}")
                        if 'all_time_low' in values and min_all is not None and values['all_time_low'] - tol > min_all:
                            issues.append(f"all_time_low {values['all_time_low']:.1f} > daily min all_time {min_all:.1f}")

                # Check highs
                high_hierarchy = ['90d_high', 'ytd_high', '52w_high', 'all_time_high']
                available_highs = [(period, values[period]) for period in high_hierarchy if period in values]
                for i in range(len(available_highs) - 1):
                    current_period, current_value = available_highs[i]
                    next_period, next_value = available_highs[i + 1]
                    if current_value > next_value:
                        issues.append(f"{current_period} ({current_value}) > {next_period} ({next_value})")

                # Check lows
                low_hierarchy = ['90d_low', 'ytd_low', '52w_low', 'all_time_low']
                available_lows = [(period, values[period]) for period in low_hierarchy if period in values]
                for i in range(len(available_lows) - 1):
                    current_period, current_value = available_lows[i]
                    next_period, next_value = available_lows[i + 1]
                    if current_value < next_value:
                        issues.append(f"{current_period} ({current_value}) < {next_period} ({next_value})")

                # If we found inconsistencies, add anomaly
                if issues:
                    anomalies.append({
                        "type": "price_data_inconsistency",
                        "symbol": symbol,
                        "issues": issues,
                        "values": values,
                        "message": f"symbol {symbol}: Price data inconsistencies detected - {'; '.join(issues)}",
                        "severity": "error"
                    })
        except Exception as e:
            anomalies.append({
                "type": "validation_error",
                "message": f"Error validating all-time price data: {str(e)}",
                "severity": "error"
            })
        return {"anomalies": anomalies}
    
    async def _validate_filings(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate idx_filings table
        Conditions:
        1. Compare filing price with daily price at the timestamp from idx_daily_data
        2. Detect duplicate transactions: same amount_transaction AND (date difference < 3 days OR same holder_name)
        """
        anomalies = []
        try:
            # Ensure required columns for price validation
            required_cols = ['timestamp', 'tickers', 'price']
            missing_cols = [col for col in required_cols if col not in data.columns]
            if missing_cols:
                anomalies.append({
                    "type": "missing_required_columns",
                    "columns": missing_cols,
                    "message": f"Missing required columns: {', '.join(missing_cols)}",
                    "severity": "error"
                })
                return {"anomalies": anomalies}

            # Prepare data
            data = data.copy()
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            data['date'] = data['timestamp'].dt.date

            # Rule 1: Compare filing price with daily price
            for idx, filing in data.iterrows():
                try:
                    # Validate price
                    if pd.isna(filing['price']) or filing['price'] == '':
                        continue
                    filing_price = float(filing['price'])
                    filing_date = filing['date']

                    # Handle tickers as list or string
                    tickers = filing['tickers']
                    if isinstance(tickers, str):
                        try:
                            tickers = eval(tickers) if tickers.startswith('[') else [tickers]
                        except:
                            tickers = [tickers]
                    elif not isinstance(tickers, list):
                        continue

                    for ticker in tickers:
                        try:
                            daily_data = await self._fetch_ticker_data('idx_daily_data', ticker)
                        except Exception:
                            continue
                        if daily_data is None or daily_data.empty:
                            continue
                        daily_data = daily_data.copy()
                        daily_data['date'] = pd.to_datetime(daily_data['date']).dt.date
                        matching_daily = daily_data[daily_data['date'] == filing_date]
                        if matching_daily.empty:
                            continue
                        daily_close = float(matching_daily.iloc[0]['close'])
                        price_diff_pct = abs(filing_price - daily_close) / daily_close * 100
                        if price_diff_pct >= 50:
                            anomalies.append({
                                "type": "filing_price_discrepancy",
                                "ticker": ticker,
                                "filing_date": filing_date.strftime('%Y-%m-%d'),
                                "filing_timestamp": filing['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                                "filing_price": filing_price,
                                "daily_close_price": daily_close,
                                "price_difference_pct": round(price_diff_pct, 2),
                                "message": f"Ticker {ticker} on {filing_date}: Filing price differs significantly from daily close",
                                "severity": "warning"
                            })
                except (ValueError, TypeError):
                    continue

            # Rule 2: Detect duplicate transactions
            # Check if required columns exist
            duplicate_check_cols = ['amount_transaction', 'timestamp', 'holder_name', 'symbol']
            if all(col in data.columns for col in duplicate_check_cols):
                # Filter out rows with null amount_transaction
                valid_data = data[data['amount_transaction'].notna()].copy()
                
                if len(valid_data) > 1:
                    # Track which transaction IDs we've already reported as duplicates
                    reported_groups = set()
                    
                    # Compare each transaction with others
                    for i in range(len(valid_data)):
                        row_a = valid_data.iloc[i]
                        id_a = self._to_json_serializable(row_a.get('id', i))
                        
                        # Skip if this transaction is already in a reported group
                        if id_a in reported_groups:
                            continue
                        
                        for j in range(i + 1, len(valid_data)):
                            row_b = valid_data.iloc[j]
                            id_b = self._to_json_serializable(row_b.get('id', j))
                            
                            # Skip if already reported
                            if id_b in reported_groups:
                                continue
                            
                            # Must be same symbol
                            symbol_a = str(row_a.get('symbol', '')).strip().upper()
                            symbol_b = str(row_b.get('symbol', '')).strip().upper()
                            if symbol_a != symbol_b or not symbol_a:
                                continue
                            
                            # Check if amount_transaction is the same
                            if row_a['amount_transaction'] == row_b['amount_transaction']:
                                # Calculate date difference in days
                                date_diff = abs((row_a['timestamp'] - row_b['timestamp']).days)
                                same_holder = str(row_a.get('holder_name', '')).strip().lower() == str(row_b.get('holder_name', '')).strip().lower()
                                
                                # Check if date difference < 3 days OR same holder (but only if date diff <= 3)
                                # This prevents false positives from transactions many months/years apart
                                if date_diff < 3 or (same_holder and date_diff <= 3):
                                    # Mark both as reported
                                    reported_groups.add(id_a)
                                    reported_groups.add(id_b)
                                    
                                    # Convert all values to JSON serializable types
                                    anomalies.append({
                                        "type": "duplicate_transaction",
                                        "transaction_1_id": id_a,
                                        "transaction_2_id": id_b,
                                        "amount_transaction": self._to_json_serializable(row_a['amount_transaction']),
                                        "holder_1": str(row_a.get('holder_name', 'N/A')),
                                        "holder_2": str(row_b.get('holder_name', 'N/A')),
                                        "date_1": row_a['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(row_a['timestamp'], 'strftime') else str(row_a['timestamp']),
                                        "date_2": row_b['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(row_b['timestamp'], 'strftime') else str(row_b['timestamp']),
                                        "date_difference_days": int(date_diff),
                                        "same_holder": bool(same_holder),
                                        "symbol": symbol_a,
                                        "message": f"Potential duplicate transaction detected: Same amount ({int(row_a['amount_transaction']):,}) shares for {symbol_a}, {date_diff} day(s) apart" + 
                                                  (f", same holder ({row_a.get('holder_name', 'N/A')})" if same_holder else ""),
                                        "severity": "error"
                                    })
                                    # Only report first match for transaction_a, then move to next transaction
                                    break

        except Exception as e:
            anomalies.append({
                "type": "validation_error",
                "message": f"Error validating filing data: {str(e)}",
                "severity": "error"
            })
        return {"anomalies": anomalies}
    
    async def _validate_stock_split(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate idx_stock_split table
        Condition: Check if there are 2 stock splits within 2 weeks for the same symbol
        """
        anomalies = []
        try:
            # Ensure required columns
            required_cols = ['symbol', 'date', 'split_ratio']
            missing_cols = [col for col in required_cols if col not in data.columns]
            if missing_cols:
                anomalies.append({
                    "type": "missing_required_columns",
                    "columns": missing_cols,
                    "message": f"Missing required columns: {', '.join(missing_cols)}",
                    "severity": "error"
                })
                return {"anomalies": anomalies}

            data = data.copy()
            data['date'] = pd.to_datetime(data['date'])
            
            # Group by symbol and check for close splits
            for symbol in data['symbol'].unique():
                symbol_data = data[data['symbol'] == symbol].sort_values('date')
                
                if len(symbol_data) < 2:
                    continue  # Need at least 2 splits to compare
                
                # Check each pair of consecutive splits
                for i in range(len(symbol_data) - 1):
                    current_split = symbol_data.iloc[i]
                    next_split = symbol_data.iloc[i + 1]
                    
                    # Calculate time difference
                    time_diff = next_split['date'] - current_split['date']
                    days_diff = time_diff.days
                    
                    # Check if within 2 weeks (14 days)
                    if days_diff <= 14:
                        anomalies.append({
                            "type": "close_stock_splits",
                            "symbol": symbol,
                            "first_split_date": current_split['date'].strftime('%Y-%m-%d'),
                            "second_split_date": next_split['date'].strftime('%Y-%m-%d'),
                            "days_between": days_diff,
                            "first_split_ratio": float(current_split['split_ratio']),
                            "second_split_ratio": float(next_split['split_ratio']),
                            "message": f"Symbol {symbol}: Two stock splits occurred within a short timeframe",
                            "severity": "warning"
                        })
        except Exception as e:
            anomalies.append({
                "type": "validation_error",
                "message": f"Error validating stock split data: {str(e)}",
                "severity": "error"
            })
        return {"anomalies": anomalies}

    async def _validate_news(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Validate idx_news table.
        Rules per row:
        1. Column 'sub_sector' must exist and be a list (or JSON array string) of unique strings.
        2. Length of the sub_sector list must be <= 5.
        3. No duplicate (case-insensitive) entries inside the list.
        """
        anomalies: List[Dict[str, Any]] = []
        try:
            if 'sub_sector' not in data.columns:
                anomalies.append({
                    "type": "missing_required_columns",
                    "columns": ['sub_sector'],
                    "message": "Missing required column: sub_sector",
                    "severity": "error"
                })
                return {"anomalies": anomalies}

            for idx, row in data.iterrows():
                raw_val = row.get('sub_sector')
                parsed_list: List[str] = []
                record_id = row.get('id')

                if isinstance(raw_val, list):
                    parsed_list = raw_val
                elif isinstance(raw_val, str):
                    val = raw_val.strip()
                    if val.startswith('[') and val.endswith(']'):
                        try:
                            import json
                            parsed = json.loads(val)
                            if isinstance(parsed, list):
                                parsed_list = parsed
                            else:
                                parsed_list = [str(parsed)]
                        except Exception:
                            inner = val[1:-1]
                            parsed_list = [p.strip().strip('"\'') for p in inner.split(',') if p.strip()]
                    elif val == '' or val.lower() in ('none','null'):
                        parsed_list = []
                    else:
                        if ',' in val:
                            parsed_list = [p.strip() for p in val.split(',') if p.strip()]
                        else:
                            parsed_list = [val]
                elif pd.isna(raw_val):
                    parsed_list = []
                else:
                    anomalies.append({
                        "type": "invalid_sub_sector_format",
                        "row_index": int(idx),
                        "value": str(raw_val),
                        "id": record_id,
                        "message": "Subsector field has unsupported type",
                        "severity": "error"
                    })
                    continue

                normalized = [str(item).strip() for item in parsed_list if item not in (None, '')]

                if len(normalized) > 5:
                    anomalies.append({
                        "type": "invalid_subsector_length",
                        "row_index": int(idx),
                        "length": len(normalized),
                        "id": record_id,
                        "message": f"Subsector list length {len(normalized)} exceeds maximum 5",
                        "severity": "error"
                    })

                lowered_seen = {}
                duplicates = set()
                for item in normalized:
                    key = item.lower()
                    if key in lowered_seen:
                        duplicates.add(item)
                    else:
                        lowered_seen[key] = True
                if duplicates:
                    anomalies.append({
                        "type": "duplicate_subsector_entries",
                        "row_index": int(idx),
                        "duplicates": sorted(list(duplicates)),
                        "id": record_id,
                        "message": f"Duplicate subsector entries found: {', '.join(sorted(list(duplicates)))}",
                        "severity": "error"
                    })
        except Exception as e:
            anomalies.append({
                "type": "validation_error",
                "message": f"Error validating idx_news data: {str(e)}",
                "severity": "error"
            })
        return {"anomalies": anomalies}

    async def _validate_sgx_manual_input(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate SGX manual input data (Top 50 companies by market cap) with two specific rules:
        1. industry_breakdown.customer_breakdown.sum(revenue) <= income_stmt_metrics.total_revenue
        2. industry_breakdown.property_counts_by_country.sum(value1) <= income_stmt_metrics.total_revenue
        """
        anomalies = []
        
        try:
            # Convert DataFrame to list of dictionaries
            if isinstance(data, pd.DataFrame):
                data_list = data.to_dict('records')
                print(f"🏢 [SGX Manual Input Validator] Validating {len(data_list)} companies (top 50 by market cap)")
            else:
                data_list = data  # Backward compatibility
                print(f"🏢 [SGX Manual Input Validator] Validating {len(data_list)} companies")
            
            for record in data_list:
                symbol = record.get('symbol', 'Unknown')
                financial_year = record.get('financial_year', 'Unknown')
                
                # Get the reference total revenue
                income_stmt = record.get('income_stmt_metrics', {})
                total_revenue = income_stmt.get('total_revenue')
                
                if total_revenue is None:
                    anomalies.append({
                        "type": "missing_required_data",
                        "symbol": symbol,
                        "financial_year": financial_year,
                        "message": f"Missing income_stmt_metrics.total_revenue for {symbol} ({financial_year})",
                        "severity": "error"
                    })
                    continue
                
                industry_breakdown = record.get('industry_breakdown', {})
                
                # Validation Rule 1: customer_breakdown sum <= total_revenue
                customer_breakdown = industry_breakdown.get('customer_breakdown', {})
                if customer_breakdown:
                    try:
                        customer_sum = 0
                        customer_details = []
                        
                        # Handle different data types in customer_breakdown
                        if isinstance(customer_breakdown, dict):
                            for customer_type, value in customer_breakdown.items():
                                if isinstance(value, (int, float)) and value is not None:
                                    customer_sum += value
                                    customer_details.append(f"{customer_type}: {value:,.0f}")
                                elif isinstance(value, list) and len(value) > 0:
                                    # Handle array format - sum all numeric values
                                    for item in value:
                                        if isinstance(item, (int, float)) and item is not None:
                                            customer_sum += item
                                    customer_details.append(f"{customer_type}: {value}")
                        
                        if customer_sum > total_revenue:
                            anomalies.append({
                                "type": "business_rule_violation",
                                "symbol": symbol,
                                "financial_year": financial_year,
                                "metric": "customer_breakdown_sum",
                                "message": f"Customer breakdown sum exceeds total revenue for {symbol} ({financial_year})",
                                "customer_breakdown_sum": customer_sum,
                                "total_revenue": total_revenue,
                                "difference": customer_sum - total_revenue,
                                "difference_pct": ((customer_sum - total_revenue) / total_revenue * 100) if total_revenue != 0 else 0,
                                "customer_details": customer_details[:3],
                                "severity": "error"
                            })
                    except Exception as e:
                        anomalies.append({
                            "type": "validation_error",
                            "symbol": symbol,
                            "financial_year": financial_year,
                            "message": f"Error processing customer_breakdown for {symbol} ({financial_year}): {str(e)}",
                            "severity": "error"
                        })
                
                # Validation Rule 2: property_counts_by_country sum(value1) <= total_revenue
                property_counts = industry_breakdown.get('property_counts_by_country', {})
                if property_counts:
                    try:
                        property_sum = 0
                        property_details = []
                        
                        for country, properties in property_counts.items():
                            if isinstance(properties, dict):
                                for property_type, property_data in properties.items():
                                    if isinstance(property_data, list) and len(property_data) >= 2:
                                        # Extract value1 (index 1) from [count, value1, value2]
                                        value1 = property_data[1]
                                        if isinstance(value1, (int, float)) and value1 is not None:
                                            property_sum += value1
                                            property_details.append(f"{country}-{property_type}: {value1:,.0f}")
                        
                        if property_sum > total_revenue:
                            anomalies.append({
                                "type": "business_rule_violation", 
                                "symbol": symbol,
                                "financial_year": financial_year,
                                "metric": "property_counts_sum",
                                "message": f"Property counts sum exceeds total revenue for {symbol} ({financial_year})",
                                "property_counts_sum": property_sum,
                                "total_revenue": total_revenue,
                                "difference": property_sum - total_revenue,
                                "difference_pct": ((property_sum - total_revenue) / total_revenue * 100) if total_revenue != 0 else 0,
                                "property_details": property_details[:3],
                                "severity": "error"
                            })
                    except Exception as e:
                        anomalies.append({
                            "type": "validation_error",
                            "symbol": symbol,
                            "financial_year": financial_year,
                            "message": f"Error processing property_counts_by_country for {symbol} ({financial_year}): {str(e)}",
                            "severity": "error"
                        })
                        
        except Exception as e:
            anomalies.append({
                "type": "validation_error",
                "message": f"Error validating SGX manual input data: {str(e)}",
                "severity": "error"
            })
            
        return {"anomalies": anomalies}

    async def _validate_company_profile(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate idx_company_profile table
        Condition: Check if shareholders share_percentage sums to 100% (with 1% tolerance)
        """
        anomalies = []
        try:
            # Ensure required columns
            required_cols = ['symbol', 'shareholders']
            missing_cols = [col for col in required_cols if col not in data.columns]
            if missing_cols:
                anomalies.append({
                    "type": "missing_required_columns",
                    "columns": missing_cols,
                    "message": f"Missing required columns: {', '.join(missing_cols)}",
                    "severity": "error"
                })
                return {"anomalies": anomalies}

            for idx, row in data.iterrows():
                symbol = row.get('symbol')
                
                # Validate shareholders
                shareholders = row.get('shareholders')
                if shareholders is None or (isinstance(shareholders, float) and pd.isna(shareholders)):
                    anomalies.append({
                        "type": "missing_shareholders",
                        "symbol": symbol,
                        "message": f"Symbol {symbol} has no shareholders data",
                        "severity": "warning"
                    })
                    continue
                
                # Parse shareholders
                shareholders_list = []
                if isinstance(shareholders, str):
                    try:
                        shareholders_list = json.loads(shareholders)
                    except json.JSONDecodeError:
                        anomalies.append({
                            "type": "invalid_shareholders_format",
                            "symbol": symbol,
                            "message": f"Symbol {symbol} has invalid shareholders JSON format",
                            "severity": "error"
                        })
                        continue
                elif isinstance(shareholders, list):
                    shareholders_list = shareholders
                else:
                    anomalies.append({
                        "type": "invalid_shareholders_type",
                        "symbol": symbol,
                        "message": f"Symbol {symbol} has invalid shareholders data type (expected list or JSON string)",
                        "severity": "error"
                    })
                    continue
                
                if not shareholders_list:
                    anomalies.append({
                        "type": "empty_shareholders",
                        "symbol": symbol,
                        "message": f"Symbol {symbol} has empty shareholders list",
                        "severity": "warning"
                    })
                    continue
                
                # Calculate total share_percentage
                total_percentage = 0.0
                invalid_entries = []
                
                for i, shareholder in enumerate(shareholders_list):
                    if not isinstance(shareholder, dict):
                        invalid_entries.append(f"Entry {i+1} is not a valid object")
                        continue
                    
                    share_pct = shareholder.get('share_percentage')
                    if share_pct is None:
                        invalid_entries.append(f"{shareholder.get('name', f'Entry {i+1}')} missing share_percentage")
                        continue
                    
                    try:
                        share_pct_float = float(share_pct)
                        total_percentage += share_pct_float
                    except (ValueError, TypeError):
                        invalid_entries.append(f"{shareholder.get('name', f'Entry {i+1}')} has invalid share_percentage: {share_pct}")
                
                if invalid_entries:
                    anomalies.append({
                        "type": "invalid_shareholder_entries",
                        "symbol": symbol,
                        "message": f"Symbol {symbol} has invalid shareholder entries: {'; '.join(invalid_entries)}",
                        "severity": "error"
                    })
                
                # Check if total percentage is approximately 100% (with 1% tolerance)
                # Convert to percentage if values are in decimal (0-1 range)
                if total_percentage <= 2.0:
                    total_percentage *= 100
                
                tolerance = 1.0  # 1% tolerance
                expected = 100.0
                difference = abs(total_percentage - expected)
                
                if difference > tolerance:
                    anomalies.append({
                        "type": "shareholders_percentage_mismatch",
                        "symbol": symbol,
                        "message": f"Symbol {symbol} shareholders percentage sum is {total_percentage:.2f}%, expected ~100%",
                        "total_percentage": round(total_percentage, 2),
                        "difference": round(difference, 2),
                        "severity": "error"
                    })
                    
        except Exception as e:
            anomalies.append({
                "type": "validation_error",
                "message": f"Error validating company profile data: {str(e)}",
                "severity": "error"
            })
        return {"anomalies": anomalies}