"""
Data validator with email notification integration
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from .data_validator import DataValidator
from ..notifications.email_helper import email_helper, notify_validation_result, should_notify

logger = logging.getLogger(__name__)

class NotificationDataValidator(DataValidator):
    def __init__(self, enable_notifications: bool = True):
        super().__init__()
        self.enable_notifications = enable_notifications
        
    async def validate_table(self, table_name: str, send_notification: bool = None) -> Dict[str, Any]:
        """
        validate_table method with email notifications
        
        Args:
            table_name: Name of the table to validate
            send_notification: Override for sending notifications (None = auto-decide)
            
        Returns:
            Dict containing validation results
        """
        # Run the original validation
        results = await super().validate_table(table_name)
        
        # Determine if notification should be sent
        if send_notification is None:
            send_notification = self.enable_notifications and should_notify(results)
        elif send_notification:
            send_notification = self.enable_notifications
        
        # Send notification if needed
        if send_notification:
            try:
                await self._send_validation_notification(table_name, results)
            except Exception as e:
                logger.error(f"Failed to send notification for {table_name}: {e}")
                # Don't fail the entire validation due to notification error
        
        return results
    
    async def validate_multiple_tables(self, table_names: List[str], 
                                     send_notifications: bool = True) -> Dict[str, Any]:
        """
        Validate multiple tables and optionally send summary
        
        Args:
            table_names: List of table names to validate
            send_notifications: Whether to send individual notifications
            
        Returns:
            Dict containing aggregated results
        """
        all_results = []
        summary = {
            "validation_timestamp": datetime.utcnow().isoformat(),
            "total_tables": len(table_names),
            "tables_with_issues": 0,
            "total_anomalies": 0,
            "results": {}
        }
        
        for table_name in table_names:
            try:
                result = await self.validate_table(table_name, send_notifications)
                all_results.append(result)
                summary["results"][table_name] = result
                
                if result.get("anomalies_count", 0) > 0:
                    summary["tables_with_issues"] += 1
                    summary["total_anomalies"] += result.get("anomalies_count", 0)
                    
            except Exception as e:
                logger.error(f"Error validating table {table_name}: {e}")
                error_result = {
                    "table_name": table_name,
                    "status": "error",
                    "error": str(e),
                    "validation_timestamp": datetime.utcnow().isoformat(),
                    "anomalies_count": 0,
                    "anomalies": []
                }
                all_results.append(error_result)
                summary["results"][table_name] = error_result
        
        # Store aggregated results
        await self._store_batch_validation_results(summary)
        
        return summary
    
    async def _send_validation_notification(self, table_name: str, results: Dict[str, Any]):
        """Send email notification for validation results"""
        try:
            success = await notify_validation_result(
                table_name=table_name,
                validation_results=results,
                send_email=True
            )
            
            if success:
                logger.info(f"Email notification sent for table {table_name}")
            else:
                logger.warning(f"Failed to send email notification for table {table_name}")
                
        except Exception as e:
            logger.error(f"Error sending validation notification for {table_name}: {e}")
            raise
    
    async def _store_batch_validation_results(self, summary: Dict[str, Any]):
        """Store batch validation results"""
        try:
            # Store in validation_batch_results table
            batch_data = {
                "validation_timestamp": summary["validation_timestamp"],
                "total_tables": summary["total_tables"],
                "tables_with_issues": summary["tables_with_issues"],
                "total_anomalies": summary["total_anomalies"],
                "summary_data": summary
            }
            
            self.supabase.table("validation_batch_results").insert(batch_data).execute()
            logger.info(f"Stored batch validation results for {summary['total_tables']} tables")
            
        except Exception as e:
            logger.error(f"Error storing batch validation results: {e}")

class DailyValidationRunner:
    """Class to handle daily validation runs with email summaries"""
    
    def __init__(self):
        self.validator = NotificationDataValidator(enable_notifications=True)
        
    async def run_daily_validations(self, table_names: List[str] = None) -> Dict[str, Any]:
        """
        Run daily validations for all configured tables
        
        Args:
            table_names: Specific tables to validate (None = all configured tables)
            
        Returns:
            Dict containing daily validation summary
        """
        try:
            if table_names is None:
                table_names = await self._get_configured_tables()
            
            logger.info(f"Starting daily validation for {len(table_names)} tables")
            
            # Run validations for all tables
            summary = await self.validator.validate_multiple_tables(
                table_names=table_names,
                send_notifications=True  # Send individual alerts
            )
            
            # Send daily summary email
            await self._send_daily_summary(summary)
            
            logger.info("Daily validation completed successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Error in daily validation run: {e}")
            return {
                "status": "error",
                "error": str(e),
                "validation_timestamp": datetime.utcnow().isoformat()
            }
    
    async def _get_configured_tables(self) -> List[str]:
        """Get list of tables configured for validation"""
        try:
            # Get from validation_configs table or use default list
            response = self.validator.supabase.table("validation_configs").select("table_name").execute()
            
            if response.data:
                return [row["table_name"] for row in response.data]
            else:
                # Default tables if none configured
                return [
                    "idx_financials_annual",
                    "idx_financials_quarterly", 
                    "idx_company_profile",
                    "idx_daily_prices",
                    "idx_dividend_history"
                ]
                
        except Exception as e:
            logger.error(f"Error getting configured tables: {e}")
            return []
    
    async def _send_daily_summary(self, validation_summary: Dict[str, Any]):
        """Send daily summary email"""
        try:
            # Transform data for email template
            summary_data = {
                "total_validations": validation_summary.get("total_tables", 0),
                "total_anomalies": validation_summary.get("total_anomalies", 0),
                "tables_validated": list(validation_summary.get("results", {}).keys()),
                "top_issues": self._extract_top_issues(validation_summary),
                "validation_date": datetime.now().strftime('%Y-%m-%d')
            }
            
            success = await email_helper.send_daily_summary(summary_data)
            
            if success:
                logger.info("Daily summary email sent successfully")
            else:
                logger.warning("Failed to send daily summary email")
                
        except Exception as e:
            logger.error(f"Error sending daily summary: {e}")
    
    def _extract_top_issues(self, validation_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract top issues from validation summary for email"""
        issue_counts = {}
        
        for table_name, results in validation_summary.get("results", {}).items():
            for anomaly in results.get("anomalies", []):
                issue_type = anomaly.get("type", "unknown")
                if issue_type not in issue_counts:
                    issue_counts[issue_type] = {
                        "type": issue_type,
                        "count": 0,
                        "tables": set()
                    }
                issue_counts[issue_type]["count"] += 1
                issue_counts[issue_type]["tables"].add(table_name)
        
        # Convert to list and sort by count
        top_issues = []
        for issue_type, issue_data in issue_counts.items():
            top_issues.append({
                "type": issue_type,
                "count": issue_data["count"],
                "table": ", ".join(list(issue_data["tables"])[:3])
            })
        
        top_issues.sort(key=lambda x: x["count"], reverse=True)
        return top_issues[:10]

# Convenience functions for external use
async def validate_table_with_notification(table_name: str) -> Dict[str, Any]:
    """Convenience function to validate a single table with notifications"""
    validator = NotificationDataValidator()
    return await validator.validate_table(table_name)

async def run_daily_validations(table_names: List[str] = None) -> Dict[str, Any]:
    """Convenience function to run daily validations"""
    runner = DailyValidationRunner()
    return await runner.run_daily_validations(table_names)

async def test_notification_system(table_name: str = "test_table", test_email: str = None) -> Dict[str, Any]:
    """Test the notification system with a sample validation"""
    try:
        # Create test validation results
        test_results = {
            "table_name": table_name,
            "validation_timestamp": datetime.utcnow().isoformat(),
            "total_rows": 100,
            "anomalies_count": 2,
            "anomalies": [
                {
                    "type": "null_values",
                    "message": "Null values detected in required field",
                    "severity": "medium",
                    "column": "price",
                    "count": 5
                },
                {
                    "type": "outlier_detection", 
                    "message": "Statistical outliers detected",
                    "severity": "low",
                    "column": "volume",
                    "count": 3
                }
            ],
            "status": "warning",
            "validations_performed": ["data_quality", "statistical"]
        }
        
        # Send test notification
        if test_email:
            from ..notifications.validation_email_service import validation_email_service
            success = await validation_email_service.send_validation_alert(
                table_name=table_name,
                validation_results=test_results,
                recipient_emails=[test_email]
            )
        else:
            success = await notify_validation_result(
                table_name=table_name,
                validation_results=test_results
            )
        
        return {
            "success": success,
            "test_results": test_results,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error testing notification system: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }