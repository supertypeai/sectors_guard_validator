"""
Data validation engine for different table types and validation approaches
"""

import pandas as pd
import numpy as np
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import json

from ..database.connection import get_supabase_client

class DataValidator:
    def __init__(self):
        self.supabase = get_supabase_client()
        
    async def validate_table(self, table_name: str) -> Dict[str, Any]:
        """
        Main validation method that orchestrates different validation approaches
        """
        try:
            # Get table data
            data = await self._fetch_table_data(table_name)
            
            # Determine validation approach based on table type
            validation_config = await self._get_validation_config(table_name)
            
            # Run appropriate validations
            results = {
                "table_name": table_name,
                "validation_timestamp": datetime.utcnow().isoformat(),
                "total_rows": len(data),
                "anomalies_count": 0,
                "anomalies": [],
                "status": "success",
                "validations_performed": []
            }
            
            # Statistical validation
            if "statistical" in validation_config.get("types", []):
                stat_results = await self._statistical_validation(data, table_name)
                results["validations_performed"].append("statistical")
                results["anomalies"].extend(stat_results.get("anomalies", []))
            
            # Business rule validation
            if "business_rules" in validation_config.get("types", []):
                rule_results = await self._business_rule_validation(data, validation_config.get("rules", {}))
                results["validations_performed"].append("business_rules")
                results["anomalies"].extend(rule_results.get("anomalies", []))
            
            # Data quality validation
            if "data_quality" in validation_config.get("types", []):
                quality_results = await self._data_quality_validation(data)
                results["validations_performed"].append("data_quality")
                results["anomalies"].extend(quality_results.get("anomalies", []))
            
            # Time series validation (for temporal data)
            if "time_series" in validation_config.get("types", []):
                ts_results = await self._time_series_validation(data, validation_config.get("time_column"))
                results["validations_performed"].append("time_series")
                results["anomalies"].extend(ts_results.get("anomalies", []))
            
            # Update final counts and status
            results["anomalies_count"] = len(results["anomalies"])
            
            if results["anomalies_count"] > 0:
                if results["anomalies_count"] > validation_config.get("error_threshold", 10):
                    results["status"] = "error"
                else:
                    results["status"] = "warning"
            
            # Store results
            await self._store_validation_results(results)
            
            # Create JSON file with results
            results["json_file_path"] = self._create_validation_json_file(results)
            
            return results
            
        except Exception as e:
            return {
                "table_name": table_name,
                "status": "error",
                "error": str(e),
                "validation_timestamp": datetime.utcnow().isoformat()
            }
    
    async def _fetch_table_data(self, table_name: str) -> pd.DataFrame:
        """Fetch data from Supabase table"""
        try:
            response = self.supabase.table(table_name).select("*").execute()
            return pd.DataFrame(response.data)
        except Exception as e:
            # Return empty DataFrame if table doesn't exist or error occurs
            return pd.DataFrame()

    async def _fetch_ticker_data(self, table_name: str, symbol: str) -> pd.DataFrame:
        """Fetch specific ticker data from Supabase table"""
        try:
            response = self.supabase.table(table_name).select("*").eq("symbol", symbol).execute()
            return pd.DataFrame(response.data)
        except Exception as e:
            # Return empty DataFrame if table doesn't exist or error occurs
            return pd.DataFrame()

    async def _get_company_data(self, symbol: str) -> pd.DataFrame:
        """Fetch specific company data from Supabase table"""
        try:
            response = self.supabase.table("idx_company_profile").select("*").eq("symbol", symbol).execute()
            return pd.DataFrame(response.data)
        except Exception as e:
            # Return empty DataFrame if table doesn't exist or error occurs
            return pd.DataFrame()
    
    async def _get_validation_config(self, table_name: str) -> Dict[str, Any]:
        """Get validation configuration for the table"""
        try:
            # Try to get from database first
            response = self.supabase.table("validation_configs").select("*").eq("table_name", table_name).execute()
            
            if response.data:
                row = response.data[0]
                # Base rules may be stored under different keys - prioritize config_data
                base_rules = row.get("config_data") or row.get("validation_rules") or {}

                # Merge top-level columns into the returned config so validator sees thresholds and types
                merged = dict(base_rules) if isinstance(base_rules, dict) else {}
                # error_threshold and validation_types/email_recipients may be stored as separate columns
                if "error_threshold" in row:
                    merged["error_threshold"] = row.get("error_threshold")
                if "validation_types" in row and row.get("validation_types"):
                    # normalize to types key expected by validator
                    merged["types"] = row.get("validation_types")
                if "validation_types" not in row and "types" in base_rules:
                    merged.setdefault("types", base_rules.get("types"))
                if "email_recipients" in row:
                    merged["email_recipients"] = row.get("email_recipients")

                return merged
            
            # Return default configuration
            return self._get_default_config(table_name)
        except:
            return self._get_default_config(table_name)
    
    def _get_default_config(self, table_name: str) -> Dict[str, Any]:
        """Get default validation configuration based on table name patterns"""
        if "user" in table_name.lower():
            return {
                "types": ["data_quality", "business_rules"],
                "rules": {
                    "email_format": True,
                    "required_fields": ["email", "id"],
                    "no_duplicates": ["email"]
                },
                "error_threshold": 5
            }
        elif "transaction" in table_name.lower():
            return {
                "types": ["statistical", "business_rules", "time_series"],
                "rules": {
                    "amount_range": {"min": 0, "max": 100000},
                    "required_fields": ["amount", "date", "user_id"]
                },
                "time_column": "created_at",
                "error_threshold": 10
            }
        else:
            return {
                "types": ["data_quality", "statistical"],
                "error_threshold": 5
            }
    
    async def _statistical_validation(self, data: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """Perform statistical anomaly detection"""
        anomalies = []
        
        if data.empty:
            return {"anomalies": anomalies}
        
        # Detect outliers in numerical columns
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        
        for col in numeric_columns:
            if data[col].count() > 0:
                Q1 = data[col].quantile(0.25)
                Q3 = data[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers = data[(data[col] < lower_bound) | (data[col] > upper_bound)]
                
                if len(outliers) > 0:
                    anomalies.append({
                        "type": "statistical_outlier",
                        "column": col,
                        "count": len(outliers),
                        "message": f"Found {len(outliers)} statistical outliers in column '{col}'",
                        "severity": "warning"
                    })
        
        return {"anomalies": anomalies}
    
    async def _business_rule_validation(self, data: pd.DataFrame, rules: Dict[str, Any]) -> Dict[str, Any]:
        """Validate business rules"""
        anomalies = []
        
        if data.empty:
            return {"anomalies": anomalies}
        
        # Check required fields
        if "required_fields" in rules:
            missing_fields = [field for field in rules["required_fields"] if field not in data.columns]
            if missing_fields:
                anomalies.append({
                    "type": "missing_required_fields",
                    "fields": missing_fields,
                    "message": f"Missing required fields: {', '.join(missing_fields)}",
                    "severity": "error"
                })
        
        # Check duplicates
        if "no_duplicates" in rules:
            for field in rules["no_duplicates"]:
                if field in data.columns:
                    duplicates = data[data.duplicated(subset=[field], keep=False)]
                    if len(duplicates) > 0:
                        anomalies.append({
                            "type": "duplicate_values",
                            "column": field,
                            "count": len(duplicates),
                            "message": f"Found {len(duplicates)} duplicate values in column '{field}'",
                            "severity": "warning"
                        })
        
        # Check value ranges
        if "amount_range" in rules and "amount" in data.columns:
            range_rule = rules["amount_range"]
            invalid_amounts = data[(data["amount"] < range_rule["min"]) | (data["amount"] > range_rule["max"])]
            if len(invalid_amounts) > 0:
                anomalies.append({
                    "type": "value_out_of_range",
                    "column": "amount",
                    "count": len(invalid_amounts),
                    "message": f"Found {len(invalid_amounts)} amounts outside valid range ({range_rule['min']}-{range_rule['max']})",
                    "severity": "error"
                })
        
        return {"anomalies": anomalies}
    
    async def _data_quality_validation(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Validate data quality (nulls, formats, etc.)"""
        anomalies = []
        
        if data.empty:
            return {"anomalies": anomalies}
        
        # Check for high null percentages
        for col in data.columns:
            null_percentage = (data[col].isnull().sum() / len(data)) * 100
            if null_percentage > 20:  # More than 20% nulls
                anomalies.append({
                    "type": "high_null_percentage",
                    "column": col,
                    "percentage": round(null_percentage, 2),
                    "message": f"Column '{col}' has {null_percentage:.1f}% null values",
                    "severity": "warning"
                })
        
        # Check email format if email column exists
        if "email" in data.columns:
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            invalid_emails = data[~data["email"].str.match(email_pattern, na=False)]
            if len(invalid_emails) > 0:
                anomalies.append({
                    "type": "invalid_email_format",
                    "column": "email",
                    "count": len(invalid_emails),
                    "message": f"Found {len(invalid_emails)} invalid email formats",
                    "severity": "error"
                })
        
        return {"anomalies": anomalies}
    
    async def _time_series_validation(self, data: pd.DataFrame, time_column: Optional[str]) -> Dict[str, Any]:
        """Validate time series data for trends and anomalies"""
        anomalies = []
        
        if data.empty or not time_column or time_column not in data.columns:
            return {"anomalies": anomalies}
        
        try:
            # Convert to datetime
            data[time_column] = pd.to_datetime(data[time_column])
            
            # Check for data gaps (more than 1 day without data)
            data_sorted = data.sort_values(time_column)
            time_diffs = data_sorted[time_column].diff()
            large_gaps = time_diffs[time_diffs > timedelta(days=1)]
            
            if len(large_gaps) > 0:
                anomalies.append({
                    "type": "data_gaps",
                    "column": time_column,
                    "count": len(large_gaps),
                    "message": f"Found {len(large_gaps)} significant time gaps in data",
                    "severity": "warning"
                })
            
            # Check for unusual volume changes (if we have a count or amount column)
            if "amount" in data.columns:
                daily_amounts = data.groupby(data[time_column].dt.date)["amount"].sum()
                if len(daily_amounts) > 1:
                    amount_changes = daily_amounts.pct_change().abs()
                    unusual_changes = amount_changes[amount_changes > 0.5]  # More than 50% change
                    
                    if len(unusual_changes) > 0:
                        anomalies.append({
                            "type": "unusual_volume_change",
                            "column": "amount",
                            "count": len(unusual_changes),
                            "message": f"Found {len(unusual_changes)} days with unusual volume changes",
                            "severity": "warning"
                        })
        
        except Exception as e:
            anomalies.append({
                "type": "time_series_validation_error",
                "message": f"Error in time series validation: {str(e)}",
                "severity": "error"
            })
        
        return {"anomalies": anomalies}
    
    def _create_validation_json_file(self, results: Dict[str, Any]) -> str:
        """Create JSON file for validation results with naming format [table_name]_[timestamp]_filter_[date_filter].json
        
        Returns:
            str: Absolute path to the created JSON file
        """
        import os
        from datetime import datetime
        
        # Get results folder path
        results_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), "results")
        os.makedirs(results_folder, exist_ok=True)
        
        # Generate timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate filename
        table_name = results.get("table_name", "unknown")
        date_filter = results.get("date_filter")
        
        if date_filter and (date_filter.get("start_date") or date_filter.get("end_date")):
            start = date_filter.get("start_date", "none").replace("-", "")
            end = date_filter.get("end_date", "none").replace("-", "")
            filename = f"{table_name}_{timestamp}_filter_{start}_to_{end}.json"
        else:
            filename = f"{table_name}_{timestamp}_filter_none.json"
        
        filepath = os.path.join(results_folder, filename)
        
        # Write JSON file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"Created validation JSON file: {filename}")
        return filepath
    
    async def _store_validation_results(self, results: Dict[str, Any]) -> None:
        """Store validation results in database with timeout handling"""
        try:
            # Prepare validation data
            validation_data = {
                "table_name": results["table_name"],
                "status": results["status"],
                "total_rows": results.get("total_rows", 0),
                "anomalies_count": results["anomalies_count"],
                "anomalies": results.get("anomalies", []).copy(),  # Make a copy
                "validations_performed": results.get("validations_performed", []),
                "validation_timestamp": results["validation_timestamp"]
            }
            
            # Check anomalies data size
            anomalies_size = len(json.dumps(validation_data["anomalies"]))
            print(f"📊 Storing validation results: {validation_data['table_name']}")
            print(f"   - Anomalies count: {validation_data['anomalies_count']}")
            print(f"   - Anomalies data size: {anomalies_size} chars")
            
            # If anomalies data is too large (>50KB), truncate it for db only
            if anomalies_size > 50000:
                print(f"⚠️  Anomalies data too large ({anomalies_size} chars), truncating for database...")
                original_count = len(validation_data["anomalies"])
                # Keep only first 20 anomalies
                validation_data["anomalies"] = validation_data["anomalies"][:20]
                validation_data["anomalies"].append({
                    "type": "truncated_results",
                    "message": f"Results truncated - showing first 20 out of {original_count} anomalies",
                    "severity": "info"
                })
                print(f"   - Truncated to {len(json.dumps(validation_data['anomalies']))} chars")
            
            # Attempt insert with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = self.supabase.table("validation_results").insert(validation_data).execute()
                    print(f"✅ Stored validation results for {results['table_name']} (attempt {attempt + 1})")
                    if response.data:
                        print(f"   - Inserted with ID: {response.data[0].get('id', 'unknown')}")
                    return
                except Exception as retry_error:
                    if attempt < max_retries - 1:
                        print(f"⚠️  Insert attempt {attempt + 1} failed, retrying... ({retry_error})")
                        await asyncio.sleep(1)  # Wait 1 second before retry
                    else:
                        raise retry_error
                        
        except Exception as e:
            error_msg = str(e)
            print(f"⚠️  Error storing validation results: {error_msg}")
            
            # Store results locally as fallback
            try:
                await self._store_results_locally(results)
            except Exception as local_error:
                print(f"⚠️  Local storage also failed: {local_error}")
                
            # Different error handling based on error type
            if "timed out" in error_msg.lower():
                print("💡 Database write timeout detected")
                print("   - Try reducing anomaly data size")
                print("   - Check database connection stability")
            elif "relation" in error_msg.lower() and "does not exist" in error_msg.lower():
                print("💡 validation_results table not found")
                print("   - Create the table using SQL provided earlier")
            elif "permission" in error_msg.lower() or "policy" in error_msg.lower():
                print("💡 Database permission issue")
                print("   - Check RLS policies and user permissions")
            else:
                print("💡 General database error - continuing without storing results")
            
            # Don't fail the validation process due to storage issues
            
    async def _store_results_locally(self, results: Dict[str, Any]) -> None:
        """Store validation results locally as fallback"""
        try:
            import os
            import json
            from pathlib import Path
            
            # Create local storage directory
            storage_dir = Path("validation_results_local")
            storage_dir.mkdir(exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = results["validation_timestamp"].replace(":", "-").replace(".", "-")
            filename = f"{results['table_name']}_{timestamp}.json"
            filepath = storage_dir / filename
            
            # Save to local file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            
            print(f"💾 Stored results locally: {filepath}")
            
        except Exception as e:
            print(f"⚠️  Local storage failed: {e}")
    
    def get_stored_validation_results(self) -> List[Dict[str, Any]]:
        """Get validation results from local storage"""
        try:
            import os
            import json
            from pathlib import Path
            
            results = []
            storage_dir = Path("validation_results_local")
            
            if storage_dir.exists():
                # Get all JSON files sorted by modification time (newest first)
                json_files = sorted(
                    [f for f in storage_dir.glob("*.json")],
                    key=lambda x: x.stat().st_mtime,
                    reverse=True
                )
                
                # Load up to 50 most recent results
                for filepath in json_files[:50]:
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            result = json.load(f)
                            # Add an ID based on filename for consistency
                            result['id'] = filepath.stem
                            results.append(result)
                    except Exception as e:
                        print(f"⚠️  Error loading {filepath}: {e}")
                        continue
            
            return results
            
        except Exception as e:
            print(f"⚠️  Error getting local results: {e}")
            return []
