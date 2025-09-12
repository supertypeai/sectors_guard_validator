"""
Custom IDX-specific data validators for financial data tables
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import asyncio

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
            'idx_dividend': self._validate_dividend,
            'idx_all_time_price': self._validate_all_time_price,
            'idx_filings': self._validate_filings,
            'idx_stock_split': self._validate_stock_split
        }
    
    async def validate_table(self, table_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
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
                validation_func = self.idx_tables[table_name]
                idx_results = await validation_func(data)
                results["anomalies"].extend(idx_results.get("anomalies", []))
            
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
        """Fetch data from Supabase table with optional date filtering"""
        try:
            print(f"📊 [Validator] Fetching data from table: {table_name}")
            print(f"📅 [Validator] Date filter - Start: {start_date}, End: {end_date}")
            # If no date filter provided, apply sensible defaults per-table
            today = datetime.utcnow().date()
            # Daily table: last 7 days
            if table_name == 'idx_daily_data' and not start_date and not end_date:
                default_start = (today - timedelta(days=6)).isoformat()  # last 7 days inclusive
                default_end = today.isoformat()
                start_date = default_start
                end_date = default_end
            # Quarterly financials: default to last 1 year
            elif table_name == 'idx_combine_financials_quarterly' and not start_date and not end_date:
                default_start = (today - timedelta(days=365)).isoformat()  # approx 1 year
                default_end = today.isoformat()
                start_date = default_start
                end_date = default_end

            query = self.supabase.table(table_name).select("*")
            
            # Apply date filters if provided
            if start_date:
                query = query.gte("date", start_date)
            if end_date:
                query = query.lte("date", end_date)
                
            response = query.execute()
            df = pd.DataFrame(response.data) if getattr(response, 'data', None) else pd.DataFrame()
            
            # Normalize common date aliases if present (so downstream validators can assume 'date')
            alias_date_cols = ['date', 'ex_date', 'exDate', 'ex date']
            for c in alias_date_cols:
                if c in df.columns and 'date' not in df.columns:
                    df['date'] = df[c]
                    break

            # If user requested date filtering but server-side filter returned no date column
            # (some pipelines use different column names for date), fallback to client-side filtering
            if (start_date or end_date) and ('date' not in df.columns):
                # Re-fetch without server-side date filters and filter locally
                try:
                    full_resp = self.supabase.table(table_name).select("*").execute()
                    full_df = pd.DataFrame(full_resp.data) if getattr(full_resp, 'data', None) else pd.DataFrame()
                    # Normalize aliases in the full dataset
                    for c in alias_date_cols:
                        if c in full_df.columns and 'date' not in full_df.columns:
                            full_df['date'] = full_df[c]
                            break
                    if not full_df.empty and 'date' in full_df.columns:
                        full_df['date'] = pd.to_datetime(full_df['date'], errors='coerce')
                        try:
                            if start_date:
                                full_df = full_df[full_df['date'] >= pd.to_datetime(start_date)]
                            if end_date:
                                full_df = full_df[full_df['date'] <= pd.to_datetime(end_date)]
                        except Exception:
                            pass
                        df = full_df
                except Exception as e:
                    pass

            if not df.empty and 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
            
            # Return DataFrame and the applied start/end so caller can reflect actual filter used
            return df, start_date, end_date
        except Exception as e:
            # Return empty DataFrame if table doesn't exist or error occurs
            return pd.DataFrame(), start_date, end_date
    
    def _tolerance(self, base: pd.Series, rel: float, abs_tol: float) -> pd.Series:
        """Return tolerance per-row combining relative & absolute materiality."""
        return np.maximum(base.abs() * rel, abs_tol)

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
            # Now evaluate only fully non-null rows
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
                            "severity": "warning"
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
                            "severity": "warning"
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
                "message": f"Error validating quarterly financial data: {str(e)}",
                "severity": "error"
            })
        return {"anomalies": anomalies}
    
    async def _validate_daily_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate idx_daily_data table
        Condition: close price change > 35%
        Only validate data for the last 7 days
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
            data['date'] = pd.to_datetime(data['date'])

            # Filter only last 7 days
            today = pd.Timestamp(datetime.now(tz=None).date())
            seven_days_ago = today - pd.Timedelta(days=7)
            data = data[(data['date'] >= seven_days_ago)]

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
                            "message": f"Symbol {symbol} on {row['date'].strftime('%Y-%m-%d')}: Close price changed by {row['price_pct_change']:.1f}% (close: {row['close']})",
                            "severity": "warning"
                        })
        except Exception as e:
            anomalies.append({
                "type": "validation_error",
                "message": f"Error validating daily data: {str(e)}",
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
        Condition: Compare filing price with daily price at the timestamp from idx_daily_data
        """
        anomalies = []
        try:
            # Ensure required columns
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

            # Group by ticker for consistency
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
                                "message": f"Ticker {ticker} on {filing_date}: Filing price {filing_price} differs from daily close {daily_close} by {price_diff_pct:.1f}% (>= 50%)",
                                "severity": "warning"
                            })
                except (ValueError, TypeError):
                    continue
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
                            "message": f"Symbol {symbol}: Two stock splits within {days_diff} days ({current_split['date'].strftime('%Y-%m-%d')} and {next_split['date'].strftime('%Y-%m-%d')})",
                            "severity": "warning"
                        })
        except Exception as e:
            anomalies.append({
                "type": "validation_error",
                "message": f"Error validating stock split data: {str(e)}",
                "severity": "error"
            })
        return {"anomalies": anomalies}
