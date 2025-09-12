"""
Email notification service for sending anomaly alerts
"""

import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from jinja2 import Template
from typing import Dict, List, Any
from datetime import datetime

from ..database.connection import get_supabase_client

class EmailService:
    def __init__(self):
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_username = os.getenv("SMTP_USERNAME")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        self.from_email = os.getenv("FROM_EMAIL", self.smtp_username)
        self.supabase = get_supabase_client()
    
    async def send_anomaly_alert(self, table_name: str, validation_results: Dict[str, Any]) -> bool:
        """Send email alert for detected anomalies"""
        try:
            # Get email recipients for this table
            recipients = await self._get_email_recipients(table_name)
            
            if not recipients:
                print(f"No email recipients configured for table: {table_name}")
                return False
            
            # Generate email content
            subject = f"🚨 IDX Data Validation Alert: {table_name} - {validation_results.get('anomalies_count', 0)} anomalies detected"
            html_body = self._generate_html_email(table_name, validation_results)
            text_body = self._generate_text_email(table_name, validation_results)
            
            # Send email
            success = await self._send_email(recipients, subject, html_body, text_body)
            
            # Log email attempt
            await self._log_email(validation_results.get("id"), recipients, subject, html_body, "sent" if success else "failed")
            
            return success
            
        except Exception as e:
            print(f"Error sending email alert: {e}")
            return False
    
    async def _get_email_recipients(self, table_name: str) -> List[str]:
        """Get email recipients for a specific table"""
        try:
            # Try to get from validation config
            response = self.supabase.table("validation_configs").select("email_recipients").eq("table_name", table_name).execute()
            
            if response.data and response.data[0].get("email_recipients"):
                return response.data[0]["email_recipients"]
            
        except Exception as e:
            print(f"⚠️  Could not get email recipients from config table: {e}")
            print("💡 Run 'python init_database.py' to create the validation_configs table")
        
        # Return default recipients from environment
        default_emails = os.getenv("DEFAULT_EMAIL_RECIPIENTS", "")
        if default_emails:
            recipients = [email.strip() for email in default_emails.split(",") if email.strip()]
            print(f"📧 Using default email recipients: {recipients}")
            return recipients
        
        print(f"⚠️  No email recipients configured for table: {table_name}")
        print("💡 Set DEFAULT_EMAIL_RECIPIENTS environment variable or run 'python init_database.py'")
        return []
    
    def _generate_html_email(self, table_name: str, validation_results: Dict[str, Any]) -> str:
        """Generate HTML email content"""
        template = Template("""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .header { background-color: #f44336; color: white; padding: 15px; border-radius: 5px; }
                .content { margin: 20px 0; }
                .anomaly { background-color: #fff3cd; border: 1px solid #ffeaa7; padding: 10px; margin: 10px 0; border-radius: 5px; }
                .stats { background-color: #e3f2fd; padding: 15px; border-radius: 5px; margin: 15px 0; }
                .footer { color: #666; font-size: 12px; margin-top: 30px; }
                table { border-collapse: collapse; width: 100%; margin: 15px 0; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🚨 Data Validation Alert</h2>
                <p>Anomalies detected in table: <strong>{{ table_name }}</strong></p>
            </div>
            
            <div class="content">
                <div class="stats">
                    <h3>📊 Validation Summary</h3>
                    <ul>
                        <li><strong>Table:</strong> {{ table_name }}</li>
                        <li><strong>Status:</strong> {{ status }}</li>
                        <li><strong>Total Rows Analyzed:</strong> {{ total_rows }}</li>
                        <li><strong>Anomalies Found:</strong> {{ anomalies_count }}</li>
                        <li><strong>Validation Time:</strong> {{ validation_timestamp }}</li>
                        <li><strong>Validations Performed:</strong> {{ validations_performed|join(', ') }}</li>
                    </ul>
                </div>
                
                {% if anomalies %}
                <h3>🔍 Detected Anomalies</h3>
                {% for anomaly in anomalies %}
                <div class="anomaly">
                    <h4>{{ anomaly.type|title|replace('_', ' ') }}</h4>
                    <p><strong>Message:</strong> {{ anomaly.message }}</p>
                    <p><strong>Severity:</strong> {{ anomaly.severity|upper }}</p>
                    {% if anomaly.column %}
                    <p><strong>Column:</strong> {{ anomaly.column }}</p>
                    {% endif %}
                    {% if anomaly.count %}
                    <p><strong>Affected Records:</strong> {{ anomaly.count }}</p>
                    {% endif %}
                </div>
                {% endfor %}
                {% endif %}
                
                <div style="margin-top: 20px;">
                    <p><strong>Next Steps:</strong></p>
                    <ol>
                        <li>Review the anomalies detected in the {{ table_name }} table</li>
                        <li>Check the dashboard for detailed charts and trends</li>
                        <li>Investigate the root cause of the data quality issues</li>
                        <li>Take appropriate corrective actions</li>
                    </ol>
                </div>
            </div>
            
            <div class="footer">
                <p>This is an automated message from the IDX Data Validation Dashboard.</p>
                <p>Generated on {{ current_time }}</p>
            </div>
        </body>
        </html>
        """)
        
        return template.render(
            table_name=table_name,
            status=validation_results.get("status", "unknown"),
            total_rows=validation_results.get("total_rows", 0),
            anomalies_count=validation_results.get("anomalies_count", 0),
            validation_timestamp=validation_results.get("validation_timestamp", ""),
            validations_performed=validation_results.get("validations_performed", []),
            anomalies=validation_results.get("anomalies", []),
            current_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        )
    
    def _generate_text_email(self, table_name: str, validation_results: Dict[str, Any]) -> str:
        """Generate plain text email content"""
        text = f"""
DATA VALIDATION ALERT - {table_name}

Anomalies have been detected in the {table_name} table.

VALIDATION SUMMARY:
- Table: {table_name}
- Status: {validation_results.get('status', 'unknown')}
- Total Rows Analyzed: {validation_results.get('total_rows', 0)}
- Anomalies Found: {validation_results.get('anomalies_count', 0)}
- Validation Time: {validation_results.get('validation_timestamp', '')}
- Validations Performed: {', '.join(validation_results.get('validations_performed', []))}

DETECTED ANOMALIES:
"""
        
        for i, anomaly in enumerate(validation_results.get("anomalies", []), 1):
            text += f"\n{i}. {anomaly.get('type', '').replace('_', ' ').title()}\n"
            text += f"   Message: {anomaly.get('message', '')}\n"
            text += f"   Severity: {anomaly.get('severity', '').upper()}\n"
            if anomaly.get('column'):
                text += f"   Column: {anomaly.get('column')}\n"
            if anomaly.get('count'):
                text += f"   Affected Records: {anomaly.get('count')}\n"
        
        text += f"""

NEXT STEPS:
1. Review the anomalies detected in the {table_name} table
2. Check the dashboard for detailed charts and trends
3. Investigate the root cause of the data quality issues
4. Take appropriate corrective actions

This is an automated message from the IDX Data Validation Dashboard.
Generated on {datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")}
"""
        
        return text
    
    async def _send_email(self, recipients: List[str], subject: str, html_body: str, text_body: str) -> bool:
        """Send email using SMTP"""
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.from_email
            msg['To'] = ', '.join(recipients)
            
            # Create text and HTML parts
            text_part = MIMEText(text_body, 'plain')
            html_part = MIMEText(html_body, 'html')
            
            # Add parts to message
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                if self.smtp_username and self.smtp_password:
                    server.login(self.smtp_username, self.smtp_password)
                server.send_message(msg)
            
            print(f"Email sent successfully to {recipients}")
            return True
            
        except Exception as e:
            print(f"Failed to send email: {e}")
            return False
    
    async def _log_email(self, validation_result_id: int, recipients: List[str], subject: str, body: str, status: str) -> None:
        """Log email sending attempt"""
        try:
            email_log = {
                "validation_result_id": validation_result_id,
                "recipients": recipients,
                "subject": subject,
                "body": body[:1000],  # Truncate body for storage
                "sent_at": datetime.utcnow().isoformat(),
                "status": status
            }
            
            self.supabase.table("email_logs").insert(email_log).execute()
        except Exception as e:
            print(f"Error logging email: {e}")

    async def send_daily_summary(self, summary_data: Dict[str, Any]) -> bool:
        """Send daily validation summary email"""
        try:
            recipients = os.getenv("DAILY_SUMMARY_RECIPIENTS", "").split(",")
            recipients = [email.strip() for email in recipients if email.strip()]
            
            if not recipients:
                return False
            
            subject = f"📊 Daily Data Validation Summary - {datetime.now().strftime('%Y-%m-%d')}"
            html_body = self._generate_daily_summary_html(summary_data)
            text_body = self._generate_daily_summary_text(summary_data)
            
            return await self._send_email(recipients, subject, html_body, text_body)
            
        except Exception as e:
            print(f"Error sending daily summary: {e}")
            return False
    
    def _generate_daily_summary_html(self, summary_data: Dict[str, Any]) -> str:
        """Generate HTML for daily summary email"""
        template = Template("""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .header { background-color: #4CAF50; color: white; padding: 15px; border-radius: 5px; }
                .stats { background-color: #e8f5e8; padding: 15px; border-radius: 5px; margin: 15px 0; }
                .table-summary { margin: 20px 0; }
                table { border-collapse: collapse; width: 100%; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
                .success { color: #4CAF50; }
                .warning { color: #FF9800; }
                .error { color: #f44336; }
            </style>
        </head>
        <body>
            <div class="header">
                <h2>📊 Daily Data Validation Summary</h2>
                <p>{{ date }}</p>
            </div>
            
            <div class="stats">
                <h3>Overall Statistics</h3>
                <ul>
                    <li><strong>Total Validations:</strong> {{ total_validations }}</li>
                    <li><strong>Tables Validated:</strong> {{ tables_validated }}</li>
                    <li><strong>Total Anomalies:</strong> {{ total_anomalies }}</li>
                    <li><strong>Emails Sent:</strong> {{ emails_sent }}</li>
                </ul>
            </div>
            
            <div class="table-summary">
                <h3>Table Status Summary</h3>
                <table>
                    <tr>
                        <th>Table Name</th>
                        <th>Status</th>
                        <th>Anomalies</th>
                        <th>Last Validated</th>
                    </tr>
                    {% for table in table_summaries %}
                    <tr>
                        <td>{{ table.name }}</td>
                        <td class="{{ table.status }}">{{ table.status|upper }}</td>
                        <td>{{ table.anomalies }}</td>
                        <td>{{ table.last_validated }}</td>
                    </tr>
                    {% endfor %}
                </table>
            </div>
        </body>
        </html>
        """)
        
        return template.render(**summary_data)
    
    def _generate_daily_summary_text(self, summary_data: Dict[str, Any]) -> str:
        """Generate plain text for daily summary email"""
        text = f"""
DAILY DATA VALIDATION SUMMARY - {summary_data.get('date', '')}

OVERALL STATISTICS:
- Total Validations: {summary_data.get('total_validations', 0)}
- Tables Validated: {summary_data.get('tables_validated', 0)}
- Total Anomalies: {summary_data.get('total_anomalies', 0)}
- Emails Sent: {summary_data.get('emails_sent', 0)}

TABLE STATUS SUMMARY:
"""
        
        for table in summary_data.get('table_summaries', []):
            text += f"\n- {table.get('name', '')}: {table.get('status', '').upper()} ({table.get('anomalies', 0)} anomalies)"
        
        text += "\n\nThis is an automated daily summary from the IDX Data Validation Dashboard."
        
        return text
