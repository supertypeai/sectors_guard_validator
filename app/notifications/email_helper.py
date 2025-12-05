"""
Email notification integration helper for sectors guard validator
Integrates the new email service with existing validation workflows
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from .validation_email_service import ValidationEmailService
from ..config import settings

logger = logging.getLogger(__name__)

class EmailHelper:
    """Helper class to integrate email notifications with existing validation code"""
    
    def __init__(self):
        self.email_service = ValidationEmailService()
    
    async def notify_validation_complete(self, table_name: str, validation_results: Dict[str, Any], 
                                       send_email: bool = True) -> bool:
        """
        Notify about validation completion
        
        Args:
            table_name: Name of the validated table
            validation_results: Results from validation process
            send_email: Whether to send email notification
            
        Returns:
            bool: True if notification was sent successfully
        """
        if not send_email:
            return True
        
        import os
        json_file_path = validation_results.get('json_file_path')
            
        try:
            # Check if there are any flagged-level issues that warrant notification
            # Only send emails for 'flagged' severity
            anomalies = validation_results.get('anomalies', [])
            filtered_anomalies = [
                anomaly for anomaly in anomalies 
                if anomaly.get('severity', '').lower() == 'flagged'
            ]
            filtered_anomalies_count = len(filtered_anomalies)
            
            # Only send email if there are flagged-level issues 
            if filtered_anomalies_count > 0:
                success = await self.email_service.send_validation_alert(
                    table_name=table_name,
                    validation_results=validation_results,
                    json_file_path=json_file_path
                )
                
                if success:
                    logger.info(f"Validation alert sent for table {table_name} with {filtered_anomalies_count} actionable issues")
                    # Delete JSON file after successful email send
                    if json_file_path and os.path.exists(json_file_path):
                        try:
                            os.remove(json_file_path)
                            logger.info(f"Deleted validation JSON file: {json_file_path}")
                        except Exception as cleanup_error:
                            logger.warning(f"Failed to delete JSON file {json_file_path}: {cleanup_error}")
                else:
                    logger.error(f"Failed to send validation alert for table {table_name}")
                
                return success
            else:
                logger.info(f"No actionable issues detected for table {table_name} (info notifications filtered), skipping email notification")
                # Delete JSON file even if no email sent (no errors to report)
                if json_file_path and os.path.exists(json_file_path):
                    try:
                        os.remove(json_file_path)
                        logger.info(f"Deleted validation JSON file (no errors to report): {json_file_path}")
                    except Exception as cleanup_error:
                        logger.warning(f"Failed to delete JSON file {json_file_path}: {cleanup_error}")
                return True
                
        except Exception as e:
            logger.error(f"Error in notify_validation_complete for {table_name}: {e}")
            # Cleanup JSON file on error
            if json_file_path and os.path.exists(json_file_path):
                try:
                    os.remove(json_file_path)
                    logger.info(f"Deleted validation JSON file after error: {json_file_path}")
                except Exception as cleanup_error:
                    logger.warning(f"Failed to delete JSON file {json_file_path}: {cleanup_error}")
            return False
    
    async def send_daily_summary(self, validation_summaries: List[Dict[str, Any]]) -> bool:
        """
        Send daily summary email
        
        Args:
            validation_summaries: List of validation results from the day
            
        Returns:
            bool: True if summary was sent successfully
        """
        try:
            # Aggregate data for summary
            summary_data = self._aggregate_daily_data(validation_summaries)
            
            success = await self.email_service.send_daily_summary(summary_data)
            
            if success:
                logger.info("Daily summary email sent successfully")
            else:
                logger.error("Failed to send daily summary email")
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending daily summary: {e}")
            return False
    
    def _aggregate_daily_data(self, validation_summaries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate validation data for daily summary, excluding info severity anomalies"""
        
        total_validations = len(validation_summaries)
        
        # Filter out info severity anomalies from the count
        total_anomalies = 0
        for summary in validation_summaries:
            anomalies = summary.get('anomalies', [])
            filtered_anomalies = [
                anomaly for anomaly in anomalies 
                if anomaly.get('severity', '').lower() not in ['info', 'information']
            ]
            total_anomalies += len(filtered_anomalies)
        
        tables_validated = list(set(summary.get('table_name', 'unknown') for summary in validation_summaries))
        
        # Get top issues by frequency (excluding info severity)
        issue_counts = {}
        for summary in validation_summaries:
            for anomaly in summary.get('anomalies', []):
                # Skip info severity anomalies
                if anomaly.get('severity', '').lower() in ['info', 'information']:
                    continue
                    
                issue_type = anomaly.get('type', 'unknown')
                if issue_type not in issue_counts:
                    issue_counts[issue_type] = {
                        'type': issue_type,
                        'count': 0,
                        'tables': set()
                    }
                issue_counts[issue_type]['count'] += 1
                issue_counts[issue_type]['tables'].add(summary.get('table_name', 'unknown'))
        
        # Convert to list and sort by count
        top_issues = []
        for issue_type, issue_data in issue_counts.items():
            top_issues.append({
                'type': issue_type,
                'count': issue_data['count'],
                'table': ', '.join(list(issue_data['tables'])[:3])  # Show up to 3 tables
            })
        
        top_issues.sort(key=lambda x: x['count'], reverse=True)
        
        return {
            'total_validations': total_validations,
            'total_anomalies': total_anomalies,
            'tables_validated': tables_validated,
            'top_issues': top_issues[:10],  # Top 10 issues
            'summary_date': datetime.now().strftime('%Y-%m-%d')
        }
    
    def should_send_notification(self, validation_results: Dict[str, Any]) -> bool:
        """
        Determine if notification should be sent based on validation results
        Excludes 'info' severity anomalies from consideration
        
        Args:
            validation_results: Results from validation process
            
        Returns:
            bool: True if notification should be sent
        """
        try:
            # Filter out 'info' severity anomalies
            anomalies = validation_results.get('anomalies', [])
            filtered_anomalies = [
                anomaly for anomaly in anomalies 
                if anomaly.get('severity', '').lower() not in ['info', 'information']
            ]
            filtered_anomalies_count = len(filtered_anomalies)
            
            status = validation_results.get('status', '').lower()
            
            # Send notification if:
            # 1. There are flagged anomalies detected
            # 2. Status indicates failure or flagged
            # 3. Critical/high severity issues exist
            
            if filtered_anomalies_count > 0:
                return True
            
            if status in ['failed', 'flagged', 'critical']:
                return True
            
            # Check for critical anomalies
            anomalies = validation_results.get('anomalies', [])
            critical_anomalies = [a for a in anomalies if a.get('severity', '').lower() == 'high']
            if critical_anomalies:
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error determining notification necessity: {e}")
            return False
    
    async def test_email_configuration(self, test_email: str = None) -> Dict[str, Any]:
        """
        Test email configuration by sending a test email
        
        Args:
            test_email: Email to send test to (optional)
            
        Returns:
            dict: Test results
        """
        try:
            if not test_email:
                test_email = settings.default_email_recipients[0] if settings.default_email_recipients else None
            
            if not test_email:
                return {
                    'success': False,
                    'error': 'No test email provided and no default recipients configured'
                }
            
            # Create test validation results
            test_results = {
                'anomalies_count': 1,
                'status': 'test',
                'total_rows': 100,
                'validation_timestamp': datetime.now().isoformat(),
                'anomalies': [{
                    'type': 'test_anomaly',
                    'message': 'This is a test email notification',
                    'severity': 'low',
                    'column': 'test_column',
                    'count': 1
                }],
                'validations_performed': ['test_validation']
            }
            
            success = await self.email_service.send_validation_alert(
                table_name='test_table',
                validation_results=test_results,
                recipient_emails=[test_email]
            )
            
            return {
                'success': success,
                'test_email': test_email,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error testing email configuration: {e}")
            return {
                'success': False,
                'error': str(e)
            }

# Global helper instance
email_helper = EmailHelper()

# Convenience functions for easy integration
async def notify_validation_result(table_name: str, validation_results: Dict[str, Any], 
                                 send_email: bool = True) -> bool:
    """Convenience function to notify about validation results"""
    return await email_helper.notify_validation_complete(table_name, validation_results, send_email)

async def send_daily_validation_summary(validation_summaries: List[Dict[str, Any]]) -> bool:
    """Convenience function to send daily summary"""
    return await email_helper.send_daily_summary(validation_summaries)

def should_notify(validation_results: Dict[str, Any]) -> bool:
    """Convenience function to check if notification should be sent"""
    return email_helper.should_send_notification(validation_results)

async def test_email_setup(test_email: str = None) -> Dict[str, Any]:
    """Convenience function to test email configuration"""
    return await email_helper.test_email_configuration(test_email)