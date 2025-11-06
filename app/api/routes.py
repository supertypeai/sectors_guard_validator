"""
API routes for validation and dashboard endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Dict, Any, Optional
import time
import json
import os
import httpx
from datetime import datetime

from ..database.connection import get_supabase_client
from ..validators.idx_financial_validator import IDXFinancialValidator
from ..notifications.email_helper import EmailHelper
from app.auth import verify_bearer_token

# In-memory cache for GitHub Actions responses to avoid rate limiting
_LOCAL_CACHE = {}

validation_router = APIRouter()
dashboard_router = APIRouter()

@validation_router.get("/tables")
async def get_tables():
    """Get list of available tables for validation"""
    try:
        supabase = get_supabase_client()
        # IDX financial tables for validation
        all_tables = [
            {
                "name": "idx_combine_financials_annual", 
                "description": "Annual financial data - Revenue, earnings, assets validation",
                "validation_type": "Financial Performance (Annual)",
                "rules": "Change >50% vs average change per year"
            },
            {
                "name": "idx_combine_financials_quarterly", 
                "description": "Quarterly financial data - Revenue, earnings, assets validation", 
                "validation_type": "Financial Performance (Quarterly)",
                "rules": "Change >50% vs average change per quarter"
            },
            {
                "name": "idx_financial_sheets_annual",
                "description": "Annual financial sheets - Accounting identity validation",
                "validation_type": "Financial Sheets (Annual)",
                "rules": "Golden rules: net_income = pretax_income - taxes + minorities; minorities=0 → net_income=profit_parent; revenue>0"
            },
            {
                "name": "idx_financial_sheets_quarterly",
                "description": "Quarterly financial sheets - Accounting identity validation",
                "validation_type": "Financial Sheets (Quarterly)", 
                "rules": "Golden rules: net_income = pretax_income - taxes + minorities; minorities=0 → net_income=profit_parent; revenue>0"
            },
            {
                "name": "index_daily_data",
                "description": "Daily index level data - Expect complete coverage per day",
                "validation_type": "Index Coverage (Daily)",
                "rules": "Each date must have exactly 18 unique index_code entries"
            },
            {
                "name": "idx_daily_data", 
                "description": "Daily stock price data - Price movement monitoring (last 7 days)",
                "validation_type": "Price Movement Monitoring", 
                "rules": "Close price change >35% in last 7 days"
            },
            {
                "name": "idx_daily_data_completeness",
                "description": "Daily stock data completeness vs active symbols (yesterday, weekdays only)",
                "validation_type": "Daily Data (Completeness)",
                "rules": "Per-date coverage equals active symbols; close, volume, and market_cap are all non-null"
            },
            {
                "name": "idx_dividend", 
                "description": "Dividend data - Yield analysis and changes",
                "validation_type": "Dividend Yield Analysis",
                "rules": "Average yield ≥30% or yield change ≥10% per year"
            },
            {
                "name": "idx_all_time_price", 
                "description": "All-time price data - Price consistency validation",
                "validation_type": "Price Consistency Check",
                "rules": "Price hierarchy consistency (90d < YTD < 52w < all-time)"
            },
            {
                "name": "idx_filings", 
                "description": "Filing price validation against daily prices",
                "validation_type": "Filing Price Validation",
                "rules": "Filing price difference ≥50% vs daily close price"
            },
            {
                "name": "idx_stock_split", 
                "description": "Stock split timing validation",
                "validation_type": "Stock Split Analysis",
                "rules": "Multiple stock splits within 2 weeks for same symbol"
            },
            {
                "name": "idx_news",
                "description": "News table subsector tagging validation",
                "validation_type": "News Subsector Validation",
                "rules": "subsector list length <=5 and no duplicate entries"
            },
            {
                "name": "sgx_company_report",
                "description": "SGX company fundamentals and price freshness",
                "validation_type": "SGX Company Report Validation",
                "rules": "market_cap & volume not null; latest close date is today (UTC); historical_financials extreme change checks"
            },
            {
                "name": "sgx_manual_input",
                "description": "SGX manual input data - Business logic validation",
                "validation_type": "SGX Manual Input Validation", 
                "rules": "customer_breakdown sum <= total_revenue; property_counts sum <= total_revenue"
            },
            {
                "name": "idx_company_profile",
                "description": "Company profile validation - Shareholders percentage check",
                "validation_type": "Company Profile Validation",
                "rules": "Shareholders share_percentage sum should be ~100% (±1%); sector and industry not empty"
            },
            {
                "name": "idx_financial_sheets_annual",
                "description": "Annual financial sheets - Accounting rules validation",
                "validation_type": "Financial Sheets (Annual)",
                "rules": "1. Net Income Flow: net_income = pretax_income - income_taxes + minorities; 2. Minority Check: If minorities = 0, then net_income must equal profit_attributable_to_parent; 3. Revenue Positivity: total_revenue must always be positive"
            },
            {
                "name": "idx_financial_sheets_quarterly",
                "description": "Quarterly financial sheets - Accounting rules validation",
                "validation_type": "Financial Sheets (Quarterly)",
                "rules": "1. Net Income Flow: net_income = pretax_income - income_taxes + minorities; 2. Minority Check: If minorities = 0, then net_income must equal profit_attributable_to_parent; 3. Revenue Positivity: total_revenue must always be positive"
            }
        ]
        
        # Get last validation times from database
        for table in all_tables:
            try:
                response = supabase.table("validation_results").select("validation_timestamp").eq("table_name", table["name"]).order("validation_timestamp", desc=True).limit(1).execute()
                if response.data:
                    table["last_validated"] = response.data[0]["validation_timestamp"]
                else:
                    table["last_validated"] = None
            except Exception:
                table["last_validated"] = None
        
        return {"tables": all_tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@validation_router.post("/run/{table_name}")
async def run_validation(
    table_name: str,
    start_date: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    _: None = Depends(verify_bearer_token)
):
    """Run validation for a specific table with optional date filter"""
    try:
        print(f"🔍 [API] Running validation for table: {table_name}")
        print(f"📅 [API] Date filter - Start: {start_date}, End: {end_date}")
        
        validator = IDXFinancialValidator()
        result = await validator.validate_table(table_name, start_date=start_date, end_date=end_date)
        
        print(f"✅ [API] Validation completed for {table_name} - Status: {result.get('status')}, Anomalies: {result.get('anomalies_count', 0)}")
        
        # Send email if anomalies detected
        if result.get("anomalies_count", 0) > 0:
            email_helper = EmailHelper()
            await email_helper.notify_validation_complete(table_name, result, send_email=True)
        
        return result
    except Exception as e:
        print(f"❌ [API] Error running validation for {table_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@validation_router.post("/run-all")
async def run_all_validations(
    start_date: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    _: None = Depends(verify_bearer_token)
):
    """Run validation for all tables with optional date filter"""
    try:
        print(f"🚀 [API] Running validation for ALL tables")
        print(f"📅 [API] Date filter - Start: {start_date}, End: {end_date}")
        
        # Get all available tables. `get_tables()` returns a dict with key 'tables'.
        # Be defensive: some callers may return {'data': {'tables': [...]}} so handle both shapes.
        tables_response = await get_tables()
        if isinstance(tables_response, dict):
            if "tables" in tables_response:
                tables = tables_response["tables"]
            elif "data" in tables_response and isinstance(tables_response["data"], dict) and "tables" in tables_response["data"]:
                tables = tables_response["data"]["tables"]
            else:
                tables = []
        else:
            tables = []

        print(f"📊 [API] Found {len(tables)} tables to validate")
        
        validator = IDXFinancialValidator()
        results = []
        
        for table in tables:
            try:
                print(f"🔄 [API] Processing table: {table['name']}")
                result = await validator.validate_table(table["name"], start_date=start_date, end_date=end_date)
                results.append(result)
                
                print(f"✅ [API] Completed {table['name']} - Status: {result.get('status')}, Anomalies: {result.get('anomalies_count', 0)}")
                
                # Send email if anomalies detected
                if result.get("anomalies_count", 0) > 0:
                    email_helper = EmailHelper()
                    await email_helper.notify_validation_complete(table["name"], result, send_email=True)
                    
            except Exception as table_error:
                print(f"❌ [API] Error processing table {table['name']}: {str(table_error)}")
                results.append({
                    "table_name": table["name"],
                    "status": "error",
                    "error": str(table_error),
                    "validation_timestamp": datetime.now().isoformat()
                })
        
        # Summary
        total_tables = len(results)
        successful_validations = len([r for r in results if r.get("status") != "error"])
        total_anomalies = sum(r.get("anomalies_count", 0) for r in results)
        
        print(f"📈 [API] All validations completed - {successful_validations}/{total_tables} successful, {total_anomalies} total anomalies")
        
        return {
            "status": "success",
            "summary": {
                "total_tables": total_tables,
                "successful_validations": successful_validations,
                "total_anomalies": total_anomalies
            },
            "results": results,
            "validation_timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"❌ [API] Error running all validations: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@dashboard_router.get("/results")
async def get_validation_results(
    table_name: Optional[str] = Query(None, description="Filter by table name"),
    limit: int = Query(10, ge=1, le=100, description="Number of results to return")
):
    """Get validation results with fallback to local storage"""
    try:
        print(f"📊 [API] Getting validation results - table: {table_name}, limit: {limit}")
        
        # Try to get from database first
        supabase = get_supabase_client()
        query = supabase.table("validation_results").select("*").order("validation_timestamp", desc=True).limit(limit)
        
        if table_name:
            query = query.eq("table_name", table_name)
        
        response = query.execute()
        
        print(f"🔍 [API] Database query returned {len(response.data) if response.data else 0} results")
        
        if response.data:
            return {
                "status": "success", 
                "data": {
                    "results": response.data
                }, 
                "source": "database"
            }
            
    except Exception as db_error:
        print(f"⚠️  Database query failed: {db_error}")
    
    # Fallback to local storage
    try:
        print("📁 [API] Falling back to local storage")
        from app.validators.data_validator import DataValidator
        validator = DataValidator()
        local_results = validator.get_stored_validation_results()
        
        # Filter by table_name if specified
        if table_name:
            local_results = [r for r in local_results if r.get("table_name") == table_name]
        
        # Apply limit
        local_results = local_results[:limit]
        
        print(f"📁 [API] Local storage returned {len(local_results)} results")
        
        return {
            "status": "success", 
            "data": {
                "results": local_results
            }, 
            "source": "local_storage",
            "message": "Using local storage - database unavailable"
        }
        
    except Exception as local_error:
        print(f"⚠️  Local storage also failed: {local_error}")
        return {
            "status": "error", 
            "message": "Both database and local storage unavailable",
            "data": {
                "results": []
            }
        }

@dashboard_router.get("/results/by-table/{table_name}")
async def get_validation_results_by_table(
    table_name: str,
    limit: int = Query(5, ge=1, le=100, description="Number of recent results to return")
):
    """Get recent validation results for a specific table"""
    try:
        print(f"📊 [API] Getting validation results by table - table: {table_name}, limit: {limit}")
        
        # Try to get from database first
        supabase = get_supabase_client()
        response = supabase.table("validation_results")\
            .select("*")\
            .eq("table_name", table_name)\
            .order("validation_timestamp", desc=True)\
            .limit(limit)\
            .execute()
        
        print(f"🔍 [API] Database query returned {len(response.data) if response.data else 0} results for {table_name}")
        
        if response.data:
            return {
                "status": "success",
                "data": {
                    "table_name": table_name,
                    "results": response.data,
                    "count": len(response.data)
                },
                "source": "database"
            }
        else:
            return {
                "status": "success",
                "data": {
                    "table_name": table_name,
                    "results": [],
                    "count": 0
                },
                "source": "database",
                "message": f"No validation results found for {table_name}"
            }
            
    except Exception as db_error:
        print(f"⚠️  Database query failed: {db_error}")
        
        # Fallback to local storage
        try:
            print("📁 [API] Falling back to local storage")
            from app.validators.data_validator import DataValidator
            validator = DataValidator()
            local_results = validator.get_stored_validation_results()
            
            # Filter by table_name
            filtered_results = [r for r in local_results if r.get("table_name") == table_name]
            filtered_results = filtered_results[:limit]
            
            print(f"📁 [API] Local storage returned {len(filtered_results)} results for {table_name}")
            
            return {
                "status": "success",
                "data": {
                    "table_name": table_name,
                    "results": filtered_results,
                    "count": len(filtered_results)
                },
                "source": "local_storage",
                "message": "Using local storage - database unavailable"
            }
            
        except Exception as local_error:
            print(f"⚠️  Local storage also failed: {local_error}")
            return {
                "status": "error",
                "message": "Both database and local storage unavailable",
                "data": {
                    "table_name": table_name,
                    "results": [],
                    "count": 0
                }
            }

@dashboard_router.get("/stats")
async def get_dashboard_stats():
    """Get dashboard statistics"""
    try:
        supabase = get_supabase_client()
        
        # Get total tables count
        all_tables = [
            "idx_combine_financials_annual", "idx_combine_financials_quarterly", 
            "idx_daily_data", "idx_daily_data_completeness", "idx_dividend", "idx_all_time_price", 
            "idx_filings", "idx_stock_split", "idx_news", "sgx_company_report", "sgx_manual_input", "idx_company_profile", 
            "idx_financial_sheets_annual", "idx_financial_sheets_quarterly"
        ]
        total_tables = len(all_tables)
        
        # Get today's validations
        from datetime import datetime, timedelta
        today = datetime.now().date()
        response = supabase.table("validation_results").select("*").gte("validation_timestamp", today.isoformat()).execute()
        validated_today = len(set(result["table_name"] for result in response.data)) if response.data else 0
        
        # Get anomalies detected today
        anomalies_detected = sum(result.get("anomalies_count", 0) for result in response.data) if response.data else 0
        
        # Email logging removed - not essential for core functionality
        emails_sent = 0
        
        # Get last validation time
        last_validation_response = supabase.table("validation_results").select("validation_timestamp").order("validation_timestamp", desc=True).limit(1).execute()
        last_validation = last_validation_response.data[0]["validation_timestamp"] if last_validation_response.data else None
        
        stats = {
            "total_tables": total_tables,
            "validated_today": validated_today,
            "anomalies_detected": anomalies_detected,
            "emails_sent": emails_sent,
            "last_validation": last_validation
        }
        return stats
    except Exception as e:
        # Fallback to default stats if database query fails
        stats = {
            "total_tables": 7,
            "validated_today": 0,
            "anomalies_detected": 0,
            "emails_sent": 0,
            "last_validation": None
        }
        return stats

@dashboard_router.get("/charts/validation-trends")
async def get_validation_trends():
    """Get validation trends data for charts"""
    try:
        supabase = get_supabase_client()
        from datetime import datetime, timedelta
        
        # Get last 7 days of validation data
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
        
        dates = []
        validations = []
        anomalies = []
        
        for i in range(7):
            current_date = start_date + timedelta(days=i)
            dates.append(current_date.isoformat())
            
            # Get validations for this date
            response = supabase.table("validation_results").select("*").gte("validation_timestamp", current_date.isoformat()).lt("validation_timestamp", (current_date + timedelta(days=1)).isoformat()).execute()
            
            daily_validations = len(response.data) if response.data else 0
            daily_anomalies = sum(result.get("anomalies_count", 0) for result in response.data) if response.data else 0
            
            validations.append(daily_validations)
            anomalies.append(daily_anomalies)
        
        trends = {
            "dates": dates,
            "validations": validations,
            "anomalies": anomalies
        }
        return trends
    except Exception as e:
        # Fallback to mock data if database query fails
        from datetime import datetime, timedelta
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
        dates = [(start_date + timedelta(days=i)).isoformat() for i in range(7)]
        
        trends = {
            "dates": dates,
            "validations": [0] * 7,
            "anomalies": [0] * 7
        }
        return trends

@validation_router.get("/config/{table_name}")
async def get_table_validation_config(table_name: str):
    """Get validation configuration for a specific table"""
    try:
        supabase = get_supabase_client()

        # First try to read saved config from validation_configs table
        try:
            resp = supabase.table("validation_configs").select("*").eq("table_name", table_name).limit(1).execute()
            if resp.data and len(resp.data) > 0:
                cfg = resp.data[0]
                # Normalize shape for frontend: include config_data as validation_rules for consistency
                validation_rules = cfg.get("config_data") or cfg.get("validation_rules") or {}
                return {
                    "table_name": cfg.get("table_name"),
                    "validation_rules": validation_rules,
                    "validation_types": cfg.get("validation_types") or cfg.get("validation_types") or [],
                    "email_recipients": cfg.get("email_recipients") or cfg.get("email_recipients") or [],
                    "error_threshold": cfg.get("error_threshold") or cfg.get("error_threshold") or 5,
                    "enabled": cfg.get("enabled", True)
                }
        except Exception:
            # If DB read fails, fallback to idx_configs below
            pass

        # IDX table configurations (defaults)
        idx_configs = {
            "idx_combine_financials_annual": {
                "table_name": table_name,
                "validation_type": "Financial Performance (Annual)",
                "description": "Annual financial data validation",
                "rules": {
                    "extreme_change_threshold": 50,
                    "metrics": ["revenue", "earnings", "total_assets", "total_equity", "operating_pnl"],
                    "comparison_method": "year_over_year_percentage",
                    "alert_condition": "absolute change >50% considering average trends"
                }
            },
            "idx_combine_financials_quarterly": {
                "table_name": table_name,
                "validation_type": "Financial Performance (Quarterly)",
                "description": "Quarterly financial data validation",
                "rules": {
                    "extreme_change_threshold": 50,
                    "metrics": ["total_revenue", "earnings", "total_assets", "total_equity", "operating_pnl"],
                    "comparison_method": "quarter_over_quarter_percentage",
                    "alert_condition": "absolute change >50% considering average trends"
                }
            },
            "idx_financial_sheets_annual": {
                "table_name": table_name,
                "validation_type": "Financial Sheets (Annual)",
                "description": "Annual financial sheets accounting identity validation",
                "rules": {
                    "accounting_rules": [
                        {
                            "name": "net_income_flow",
                            "formula": "net_income = pretax_income - income_taxes + minorities",
                            "tolerance_relative": 0.001,
                            "tolerance_absolute": 1000000000
                        },
                        {
                            "name": "minority_check",
                            "formula": "if minorities = 0 then net_income = profit_attributable_to_parent",
                            "tolerance_relative": 0.001,
                            "tolerance_absolute": 1000000000
                        },
                        {
                            "name": "revenue_positivity",
                            "formula": "total_revenue > 0",
                            "strict": True
                        }
                    ],
                    "metrics": ["net_income", "pretax_income", "income_taxes", "minorities", "profit_attributable_to_parent", "total_revenue"],
                    "alert_condition": "accounting identity violation"
                }
            },
            "idx_financial_sheets_quarterly": {
                "table_name": table_name,
                "validation_type": "Financial Sheets (Quarterly)",
                "description": "Quarterly financial sheets accounting identity validation",
                "rules": {
                    "accounting_rules": [
                        {
                            "name": "net_income_flow",
                            "formula": "net_income = pretax_income - income_taxes + minorities",
                            "tolerance_relative": 0.001,
                            "tolerance_absolute": 1000000000
                        },
                        {
                            "name": "minority_check",
                            "formula": "if minorities = 0 then net_income = profit_attributable_to_parent",
                            "tolerance_relative": 0.001,
                            "tolerance_absolute": 1000000000
                        },
                        {
                            "name": "revenue_positivity",
                            "formula": "total_revenue > 0",
                            "strict": True
                        }
                    ],
                    "metrics": ["net_income", "pretax_income", "income_taxes", "minorities", "profit_attributable_to_parent", "total_revenue"],
                    "alert_condition": "accounting identity violation"
                }
            },
            "idx_daily_data": {
                "table_name": table_name,
                "validation_type": "Price Movement Monitoring",
                "description": "Daily stock price movement validation",
                "rules": {
                    "price_change_threshold": 35,
                    "time_window": "last_7_days",
                    "metrics": ["close"],
                    "alert_condition": "close price change >35% in last 7 days"
                }
            },
            "idx_dividend": {
                "table_name": table_name,
                "validation_type": "Dividend Yield Analysis",
                "description": "Dividend yield and change validation",
                "rules": {
                    "high_yield_threshold": 30,
                    "yield_change_threshold": 10,
                    "metrics": ["yield", "dividend"],
                    "alert_condition": "average yield ≥30% or yield change ≥10% per year"
                }
            },
            "idx_all_time_price": {
                "table_name": table_name,
                "validation_type": "Price Consistency Check",
                "description": "All-time price data consistency validation",
                "rules": {
                    "hierarchy_check": ["90d_high/low", "ytd_high/low", "52w_high/low", "all_time_high/low"],
                    "metrics": ["price"],
                    "alert_condition": "price hierarchy inconsistency"
                }
            },
            "idx_filings": {
                "table_name": table_name,
                "validation_type": "Filing Price Validation",
                "description": "Filing price vs daily price validation",
                "rules": {
                    "price_difference_threshold": 50,
                    "metrics": ["price"],
                    "alert_condition": "filing price difference ≥50% vs daily close price"
                }
            },
            "idx_stock_split": {
                "table_name": table_name,
                "validation_type": "Stock Split Analysis",
                "description": "Stock split timing validation",
                "rules": {
                    "time_window_threshold": 14,
                    "metrics": ["split_ratio", "date"],
                    "alert_condition": "multiple stock splits within 2 weeks for same symbol"
                }
            },
            "sgx_company_report": {
                "table_name": table_name,
                "validation_type": "SGX Company Report Validation",
                "description": "SGX company fundamentals and price freshness validation",
                "rules": {
                    "required_non_null": ["market_cap", "volume"],
                    "close_recency": "latest close date must equal today's UTC date",
                    "historical_financials_metrics": ["revenue", "earnings", "total_assets", "total_equity", "operating_pnl"],
                    "extreme_change_threshold": 100,
                    "alert_condition": "market_cap & volume not null; latest close date is today (UTC); historical_financials extreme change checks"
                }
            },
            "sgx_manual_input": {
                "table_name": table_name,
                "validation_type": "SGX Manual Input Validation",
                "description": "SGX manual input data business logic validation",
                "rules": {
                    "customer_breakdown_validation": "sum of customer_breakdown values must be <= income_stmt_metrics.total_revenue",
                    "property_counts_validation": "sum of property_counts_by_country value1 must be <= income_stmt_metrics.total_revenue",
                    "metrics": ["customer_breakdown", "property_counts_by_country", "total_revenue"],
                    "alert_condition": "customer_breakdown sum <= total_revenue; property_counts sum <= total_revenue"
                }
            }
        }
        
        if table_name in idx_configs:
            return idx_configs[table_name]
        else:
            raise HTTPException(status_code=404, detail=f"Configuration not found for table: {table_name}")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@validation_router.post("/config/{table_name}")
async def save_table_validation_config(table_name: str, payload: Dict[str, Any]):
    """Save or update validation configuration for a specific table"""
    try:
        _ = verify_bearer_token  # satisfy linters if unused in annotations
        _  # no-op
        supabase = get_supabase_client()
        print(f"💾 [API] Saving validation config for {table_name}")
        # Normalize incoming payload
        validation_rules = payload.get("validation_rules") or payload.get("config_data") or payload.get("rules") or {}
        validation_types = payload.get("validation_types") or payload.get("types") or []
        email_recipients = payload.get("email_recipients") or payload.get("emailRecipients") or []
        error_threshold = payload.get("error_threshold") or payload.get("errorThreshold") or 5
        enabled = payload.get("enabled") if "enabled" in payload else payload.get("is_active", True)

        # Check if config exists
        existing = supabase.table("validation_configs").select("*").eq("table_name", table_name).execute()

        record = {
            "table_name": table_name,
            "config_data": validation_rules,
            "validation_types": validation_types,
            "email_recipients": email_recipients,
            "error_threshold": error_threshold,
            "enabled": enabled,
        }

        if existing.data:
            # Update existing
            resp = supabase.table("validation_configs").update(record).eq("table_name", table_name).execute()
            print(f"💾 [API] Updated config for {table_name}")
        else:
            # Insert new
            resp = supabase.table("validation_configs").insert(record).execute()
            print(f"💾 [API] Inserted config for {table_name}")

        return {"status": "success", "table_name": table_name}
    except Exception as e:
        error_msg = str(e)
        print(f"❌ [API] Error saving config for {table_name}: {error_msg}")
        
        # Check if it's a column missing error and provide helpful message
        if "does not exist" in error_msg and "Column" in error_msg:
            print(f"🔧 [API] Database schema mismatch detected. Available columns may be different than expected.")
            print(f"🔧 [API] Try using 'config_data' instead of 'validation_rules' column.")
            
            # Try alternative column mapping as fallback
            try:
                alternative_record = {
                    "table_name": table_name,
                    "config_data": record.get("config_data"),
                    "email_recipients": record.get("email_recipients"),
                    "error_threshold": record.get("error_threshold"),
                    "enabled": record.get("enabled", True),
                }
                
                if existing.data:
                    resp = supabase.table("validation_configs").update(alternative_record).eq("table_name", table_name).execute()
                    print(f"💾 [API] Updated config for {table_name} using alternative schema")
                else:
                    resp = supabase.table("validation_configs").insert(alternative_record).execute()
                    print(f"💾 [API] Inserted config for {table_name} using alternative schema")
                
                return {"status": "success", "table_name": table_name, "note": "Used alternative column mapping"}
            except Exception as fallback_error:
                print(f"❌ [API] Fallback also failed: {fallback_error}")
                raise HTTPException(
                    status_code=500, 
                    detail=f"Database schema mismatch. Original error: {error_msg}. Fallback error: {str(fallback_error)}"
                )
        
        raise HTTPException(status_code=500, detail=error_msg)

@dashboard_router.get("/charts/table-status")
async def get_table_status():
    """Get table validation status for pie chart"""
    try:
        supabase = get_supabase_client()
        from datetime import datetime, timedelta
        
        # Get latest validation results for each IDX table
        all_tables = [
            "idx_combine_financials_annual", "idx_combine_financials_quarterly", 
            "idx_daily_data", "idx_daily_data_completeness", "idx_dividend", "idx_all_time_price", 
            "idx_filings", "idx_stock_split"
        ]
        
        healthy = 0
        warning = 0
        error = 0
        
        for table in all_tables:
            # Get most recent validation result for this table
            response = supabase.table("validation_results").select("status").eq("table_name", table).order("validation_timestamp", desc=True).limit(1).execute()
            
            if response.data:
                status = response.data[0]["status"]
                if status == "success":
                    healthy += 1
                elif status == "warning":
                    warning += 1
                elif status == "error":
                    error += 1
            else:
                # No validation results yet, consider as needing validation
                healthy += 1
        
        status_data = {
            "healthy": healthy,
            "warning": warning,
            "error": error
        }
        return status_data
    except Exception as e:
        # Fallback to default data if database query fails
        status_data = {
            "healthy": 7,
            "warning": 0,
            "error": 0
        }
        return status_data

@dashboard_router.get("/table-data/{table_name}")
async def get_table_data(
    table_name: str,
    symbol: Optional[str] = Query(None, description="Stock symbol to filter by"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: Optional[int] = Query(1000, description="Maximum number of records to return")
):
    """Get data from a specific table with filtering options for visualization"""
    try:
        supabase = get_supabase_client()
        
        # Valid IDX tables
        valid_tables = [
            'idx_combine_financials_annual',
            'idx_combine_financials_quarterly', 
            'idx_daily_data',
            'idx_dividend',
            'idx_all_time_price',
            'idx_stock_split',
            'idx_filings'
        ]
        
        if table_name not in valid_tables:
            raise HTTPException(status_code=400, detail=f"Invalid table name. Valid tables: {valid_tables}")
        
        # Build query
        query = supabase.table(table_name).select("*")
        
        # Apply symbol filter
        if symbol:
            if table_name == 'idx_filings':
                # For filings, check if symbol is in tickers array
                query = query.contains("tickers", [symbol])
            else:
                query = query.eq("symbol", symbol)
        
        # Apply date filters
        if start_date:
            if table_name == 'idx_filings':
                query = query.gte("timestamp", start_date)
            else:
                query = query.gte("date", start_date)
                
        if end_date:
            if table_name == 'idx_filings':
                query = query.lte("timestamp", end_date) 
            else:
                query = query.lte("date", end_date)
        
        # Apply limit
        if limit:
            query = query.limit(limit)
            
        # Order by date (most recent first)
        if table_name == 'idx_filings':
            query = query.order("timestamp", desc=True)
        else:
            query = query.order("date", desc=True)
        
        response = query.execute()
        
        if not response.data:
            return {
                "table_name": table_name,
                "data": [],
                "count": 0,
                "filters": {
                    "symbol": symbol,
                    "start_date": start_date,
                    "end_date": end_date,
                    "limit": limit
                }
            }
        
        return {
            "table_name": table_name,
            "data": response.data,
            "count": len(response.data),
            "filters": {
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching table data: {str(e)}")

@dashboard_router.get("/github-actions")
async def get_github_actions_status():
    """Get GitHub Actions workflow status for repository workflows"""
    # Return cached response when recent to avoid GitHub rate limits
    try:
        cache_entry = _LOCAL_CACHE.get('github_actions')
        if cache_entry:
            age = time.time() - cache_entry.get('ts', 0)
            if age < 86400:  # cache TTL (1 day)
                return cache_entry.get('data')
    except Exception:
        # If cache read fails, continue to fetch
        pass
    try:
        import os
        from datetime import datetime, timedelta
        
        # Get repository info from environment or defaults
        repo_owner = os.getenv("GITHUB_REPO_OWNER", "supertypeai")
        repo_name = os.getenv("GITHUB_REPO_NAME", "sectors_guard_validator")
        github_token = os.getenv("GITHUB_TOKEN")
        
        # Prepare headers
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "sectors-guard-validator"
        }
        if github_token:
            headers["Authorization"] = f"token {github_token}"
        
        # Fetch workflow runs for check-api and fetch-sheet workflows
        async with httpx.AsyncClient() as client:
            # First, try to get all workflows to see what's available
            workflows_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows"
            workflows_response = await client.get(workflows_url, headers=headers)
            
            print(f"Workflows URL: {workflows_url}")
            print(f"Workflows Response Status: {workflows_response.status_code}")
            
            if workflows_response.status_code == 200:
                workflows_data = workflows_response.json()
                print(f"Available workflows: {[w['name'] for w in workflows_data.get('workflows', [])]}")
            
            # Get check-api workflow runs
            check_api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows/check-api.yml/runs"
            check_api_response = await client.get(check_api_url, headers=headers, params={"per_page": 5})
            
            # Get fetch-sheet workflow runs  
            fetch_sheet_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows/fetch-sheet.yml/runs"
            fetch_sheet_response = await client.get(fetch_sheet_url, headers=headers, params={"per_page": 5})
            
            print(f"Check API URL: {check_api_url}")
            print(f"Check API Response Status: {check_api_response.status_code}")
            print(f"Fetch Sheet URL: {fetch_sheet_url}")
            print(f"Fetch Sheet Response Status: {fetch_sheet_response.status_code}")
            
            result = {
                "check_api": {
                    "status": "unknown",
                    "last_run": None,
                    "last_success": None,
                    "last_failure": None,
                    "runs": []
                },
                "fetch_sheet": {
                    "status": "unknown", 
                    "last_run": None,
                    "last_success": None,
                    "last_failure": None,
                    "runs": []
                }
            }
            
            # Process check-api workflow runs
            if check_api_response.status_code == 200:
                check_api_data = check_api_response.json()
                # print(f"Check API data: {check_api_data}")
                runs = check_api_data.get("workflow_runs", [])
                if runs:
                    latest_run = runs[0]
                    result["check_api"]["status"] = latest_run.get("conclusion", "unknown")
                    result["check_api"]["last_run"] = latest_run.get("created_at")
                    
                    # Find last success and failure
                    for run in runs:
                        conclusion = run.get("conclusion")
                        created_at = run.get("created_at")
                        if conclusion == "success" and not result["check_api"]["last_success"]:
                            result["check_api"]["last_success"] = created_at
                        elif conclusion == "failure" and not result["check_api"]["last_failure"]:
                            result["check_api"]["last_failure"] = created_at
                    
                    # Store recent runs for display
                    result["check_api"]["runs"] = [{
                        "id": run.get("id"),
                        "status": run.get("conclusion", "unknown"),
                        "created_at": run.get("created_at"),
                        "html_url": run.get("html_url")
                    } for run in runs[:3]]
            else:
                print(f"Check API error: {check_api_response.status_code} - {check_api_response.text}")
            
            # Process fetch-sheet workflow runs
            if fetch_sheet_response.status_code == 200:
                fetch_sheet_data = fetch_sheet_response.json()
                # print(f"Fetch Sheet data: {fetch_sheet_data}")
                runs = fetch_sheet_data.get("workflow_runs", [])
                if runs:
                    latest_run = runs[0]
                    result["fetch_sheet"]["status"] = latest_run.get("conclusion", "unknown")
                    result["fetch_sheet"]["last_run"] = latest_run.get("created_at")
                    
                    # Find last success and failure
                    for run in runs:
                        conclusion = run.get("conclusion")
                        created_at = run.get("created_at")
                        if conclusion == "success" and not result["fetch_sheet"]["last_success"]:
                            result["fetch_sheet"]["last_success"] = created_at
                        elif conclusion == "failure" and not result["fetch_sheet"]["last_failure"]:
                            result["fetch_sheet"]["last_failure"] = created_at
                    
                    # Store recent runs for display
                    result["fetch_sheet"]["runs"] = [{
                        "id": run.get("id"),
                        "status": run.get("conclusion", "unknown"),
                        "created_at": run.get("created_at"),
                        "html_url": run.get("html_url")
                    } for run in runs[:3]]
            else:
                print(f"Fetch Sheet error: {fetch_sheet_response.status_code} - {fetch_sheet_response.text}")

            # Cache the result for TTL to reduce repeated calls
            try:
                _LOCAL_CACHE['github_actions'] = {'ts': time.time(), 'data': result}
            except Exception:
                pass

            return result
            
    except Exception as e:
        # Return fallback data if GitHub API fails
        return {
            "check_api": {
                "status": "unknown",
                "last_run": None,
                "last_success": None,
                "last_failure": None,
                "runs": [],
                "error": str(e)
            },
            "fetch_sheet": {
                "status": "unknown",
                "last_run": None,
                "last_success": None,
                "last_failure": None,
                "runs": [],
                "error": str(e)
            }
        }
