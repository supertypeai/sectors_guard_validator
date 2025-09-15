"""
API routes for validation and dashboard endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
import json
from datetime import datetime

from ..database.connection import get_supabase_client
from ..validators.idx_financial_validator import IDXFinancialValidator
from ..notifications.email_service import EmailService

validation_router = APIRouter()
dashboard_router = APIRouter()

@validation_router.get("/tables")
async def get_tables():
    """Get list of available tables for validation"""
    try:
        supabase = get_supabase_client()
        # IDX financial tables for validation
        idx_tables = [
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
                "name": "idx_daily_data", 
                "description": "Daily stock price data - Price movement monitoring (last 7 days)",
                "validation_type": "Price Movement Monitoring", 
                "rules": "Close price change >35% in last 7 days"
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
            }
        ]
        
        # Get last validation times from database
        for table in idx_tables:
            try:
                response = supabase.table("validation_results").select("validation_timestamp").eq("table_name", table["name"]).order("validation_timestamp", desc=True).limit(1).execute()
                if response.data:
                    table["last_validated"] = response.data[0]["validation_timestamp"]
                else:
                    table["last_validated"] = None
            except Exception:
                table["last_validated"] = None
        
        return {"tables": idx_tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@validation_router.post("/run/{table_name}")
async def run_validation(
    table_name: str,
    start_date: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)")
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
            email_service = EmailService()
            await email_service.send_anomaly_alert(table_name, result)
        
        # Return with explicit CORS headers
        return JSONResponse(
            content=result,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Credentials": "true",
            }
        )
    except Exception as e:
        print(f"❌ [API] Error running validation for {table_name}: {str(e)}")
        # Return error with CORS headers
        return JSONResponse(
            content={"detail": str(e)},
            status_code=500,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Credentials": "true",
            }
        )

@validation_router.post("/run-all")
async def run_all_validations(
    start_date: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)")
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
                    email_service = EmailService()
                    await email_service.send_anomaly_alert(table["name"], result)
                    
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

@dashboard_router.get("/stats")
async def get_dashboard_stats():
    """Get dashboard statistics"""
    try:
        supabase = get_supabase_client()
        
        # Get total IDX tables count
        idx_tables = [
            "idx_combine_financials_annual", "idx_combine_financials_quarterly", 
            "idx_daily_data", "idx_dividend", "idx_all_time_price", 
            "idx_filings", "idx_stock_split"
        ]
        total_tables = len(idx_tables)
        
        # Get today's validations
        from datetime import datetime, timedelta
        today = datetime.now().date()
        response = supabase.table("validation_results").select("*").gte("validation_timestamp", today.isoformat()).execute()
        validated_today = len(set(result["table_name"] for result in response.data)) if response.data else 0
        
        # Get anomalies detected today
        anomalies_detected = sum(result.get("anomalies_count", 0) for result in response.data) if response.data else 0
        
        # Get emails sent today
        email_response = supabase.table("email_logs").select("*").gte("sent_at", today.isoformat()).execute()
        emails_sent = len(email_response.data) if email_response.data else 0
        
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
                # Normalize shape for frontend: include validation_rules (or config_data), types, emails, threshold
                validation_rules = cfg.get("validation_rules") or cfg.get("config_data") or cfg.get("config_data") or {}
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
        supabase = get_supabase_client()
        print(f"💾 [API] Saving validation config for {table_name}")
        # Normalize incoming payload
        validation_rules = payload.get("validation_rules") or payload.get("config_data") or payload.get("rules") or {}
        validation_types = payload.get("validation_types") or payload.get("validation_types") or payload.get("types") or []
        email_recipients = payload.get("email_recipients") or payload.get("emailRecipients") or []
        error_threshold = payload.get("error_threshold") or payload.get("errorThreshold") or payload.get("error_threshold", 5)
        enabled = payload.get("enabled") if "enabled" in payload else payload.get("is_active", True)

        # Check if config exists
        existing = supabase.table("validation_configs").select("*").eq("table_name", table_name).execute()

        record = {
            "table_name": table_name,
            "validation_rules": validation_rules,
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
        print(f"❌ [API] Error saving config for {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@dashboard_router.get("/charts/table-status")
async def get_table_status():
    """Get table validation status for pie chart"""
    try:
        supabase = get_supabase_client()
        from datetime import datetime, timedelta
        
        # Get latest validation results for each IDX table
        idx_tables = [
            "idx_combine_financials_annual", "idx_combine_financials_quarterly", 
            "idx_daily_data", "idx_dividend", "idx_all_time_price", 
            "idx_filings", "idx_stock_split"
        ]
        
        healthy = 0
        warning = 0
        error = 0
        
        for table in idx_tables:
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
