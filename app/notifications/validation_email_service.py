"""
Advanced email notification service for sectors guard validator
Based on periwatch email system with AWS SES integration and rich HTML templates
"""

import os
import boto3
import logging
import time
import threading
from datetime import datetime
from typing import Dict, List, Any, Optional
from io import BytesIO
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from botocore.exceptions import BotoCoreError, ClientError

from ..database.connection import get_supabase_client

logger = logging.getLogger(__name__)

def format_email_with_display_name(email, display_name=None):
    """Format email address with display name: 'Display Name <email@domain.com>'"""
    if not display_name:
        display_name = "Sectors Guard"
    
    if not email:
        return None
        
    # If display name contains special characters, wrap in quotes
    if any(char in display_name for char in [',', ';', '<', '>', '"', '\\']):
        display_name = f'"{display_name}"'
    
    return f"{display_name} <{email}>"

class ValidationEmailService:
    def __init__(self):
        self.supabase = get_supabase_client()
        
        # AWS SES configuration
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self.default_from_email = os.getenv('DEFAULT_FROM_EMAIL')
        self.default_from_name = os.getenv('DEFAULT_FROM_NAME', 'Sectors Guard')
        
        # SMTP fallback configuration
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_username = os.getenv("SMTP_USERNAME")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        
        self.active_tasks = {}
    
    async def send_validation_alert(self, table_name: str, validation_results: Dict[str, Any], 
                                  recipient_emails: List[str] = None) -> bool:
        """Send validation alert email with rich HTML template"""
        try:
            if not recipient_emails:
                recipient_emails = await self._get_email_recipients(table_name)
            
            if not recipient_emails:
                logger.warning(f"No email recipients configured for table: {table_name}")
                return False
            
            # Generate email content
            anomalies_count = validation_results.get('anomalies_count', 0)
            subject = f"Sectors Guard Alert: {table_name} - {anomalies_count} validation issues detected"
            
            for recipient_email in recipient_emails:
                success = await self._send_validation_email(recipient_email, table_name, validation_results)
                if success:
                    logger.info(f"Validation alert sent successfully to {recipient_email}")
                else:
                    logger.error(f"Failed to send validation alert to {recipient_email}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending validation alert: {e}")
            return False
    
    async def send_daily_summary(self, summary_data: Dict[str, Any], 
                               recipient_emails: List[str] = None) -> bool:
        """Send daily validation summary email"""
        try:
            if not recipient_emails:
                recipient_emails = self._get_default_recipients()
            
            if not recipient_emails:
                logger.warning("No email recipients configured for daily summary")
                return False
            
            subject = f"Sectors Guard Daily Summary - {datetime.now().strftime('%B %d, %Y')}"
            
            for recipient_email in recipient_emails:
                success = await self._send_summary_email(recipient_email, summary_data)
                if success:
                    logger.info(f"Daily summary sent successfully to {recipient_email}")
                else:
                    logger.error(f"Failed to send daily summary to {recipient_email}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending daily summary: {e}")
            return False
    
    async def _send_validation_email(self, recipient_email: str, table_name: str, 
                                   validation_results: Dict[str, Any]) -> bool:
        """Send validation email using AWS SES first, then SMTP fallback if SES fails."""
        try:
            return await self._send_validation_email_ses(recipient_email, table_name, validation_results)
        except Exception as ses_err:
            logger.warning(f"AWS SES failed for {recipient_email}: {ses_err}. Trying SMTP fallback...")
            return await self._send_validation_email_smtp_fallback(recipient_email, table_name, validation_results)
    
    async def _send_summary_email(self, recipient_email: str, summary_data: Dict[str, Any]) -> bool:
        """Send daily summary email using AWS SES first, then SMTP fallback if SES fails."""
        try:
            return await self._send_summary_email_ses(recipient_email, summary_data)
        except Exception as ses_err:
            logger.warning(f"AWS SES failed for {recipient_email}: {ses_err}. Trying SMTP fallback...")
            return await self._send_summary_email_smtp_fallback(recipient_email, summary_data)

    async def _send_validation_email_ses(self, recipient_email: str, table_name: str, 
                                       validation_results: Dict[str, Any]) -> bool:
        """Send validation email using AWS SES via boto3."""
        # Validate AWS settings
        if not all([self.aws_access_key_id, self.aws_secret_access_key, self.aws_region]):
            raise Exception("AWS SES credentials not configured")

        ses_client = boto3.client(
            'ses',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region,
        )

        # Build MIME message
        if not self.default_from_email:
            raise Exception("DEFAULT_FROM_EMAIL not configured")
        
        # Format sender with display name
        sender_formatted = format_email_with_display_name(self.default_from_email)
        
        # Filter to only count 'error' severity anomalies
        anomalies = validation_results.get('anomalies', [])
        filtered_anomalies_count = len([
            anomaly for anomaly in anomalies 
            if anomaly.get('severity', '').lower() == 'error'
        ])
        
        subject = f"Sectors Guard Alert: {table_name} - {filtered_anomalies_count} validation issues detected"

        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = sender_formatted
        msg['To'] = recipient_email

        # HTML body
        html_content = self._build_validation_email_html(table_name, validation_results)
        html_body = MIMEText(html_content, 'html')
        msg.attach(html_body)

        # Send raw email
        try:
            response = ses_client.send_raw_email(
                Source=sender_formatted,
                Destinations=[recipient_email],
                RawMessage={'Data': msg.as_string()}
            )
            logger.info(f"SES email sent successfully to {recipient_email}. MessageId: {response['MessageId']}")
            return True
        except (BotoCoreError, ClientError) as ses_exc:
            logger.error(f"SES send failed: {ses_exc}")
            raise ses_exc
    
    async def _send_summary_email_ses(self, recipient_email: str, summary_data: Dict[str, Any]) -> bool:
        """Send daily summary email using AWS SES via boto3."""
        # Validate AWS settings
        if not all([self.aws_access_key_id, self.aws_secret_access_key, self.aws_region]):
            raise Exception("AWS SES credentials not configured")

        ses_client = boto3.client(
            'ses',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region,
        )

        # Build MIME message
        if not self.default_from_email:
            raise Exception("DEFAULT_FROM_EMAIL not configured")
        
        # Format sender with display name
        sender_formatted = format_email_with_display_name(self.default_from_email)
        
        subject = f"Sectors Guard Daily Summary - {datetime.now().strftime('%B %d, %Y')}"

        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = sender_formatted
        msg['To'] = recipient_email

        # HTML body
        html_content = self._build_summary_email_html(summary_data)
        html_body = MIMEText(html_content, 'html')
        msg.attach(html_body)

        # Send raw email
        try:
            response = ses_client.send_raw_email(
                Source=sender_formatted,
                Destinations=[recipient_email],
                RawMessage={'Data': msg.as_string()}
            )
            logger.info(f"SES summary email sent successfully to {recipient_email}. MessageId: {response['MessageId']}")
            return True
        except (BotoCoreError, ClientError) as ses_exc:
            logger.error(f"SES send failed: {ses_exc}")
            raise ses_exc

    def _build_validation_email_html(self, table_name: str, validation_results: Dict[str, Any]) -> str:
        """Return the HTML email body for validation alerts."""
        anomalies = validation_results.get('anomalies', [])
        
        # Filter to only show 'error' severity anomalies
        filtered_anomalies = [
            anomaly for anomaly in anomalies 
            if anomaly.get('severity', '').lower() == 'error'
        ]
        filtered_anomalies_count = len(filtered_anomalies)
        
        status = validation_results.get('status', 'unknown')
        total_rows = validation_results.get('total_rows', 0)
        validation_timestamp = validation_results.get('validation_timestamp', datetime.now().isoformat())
        validations_performed = validation_results.get('validations_performed', [])
        
        # --- Icon Definitions ---
        icons = {
            'critical': '<svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="8" x2="12" y2="12"></line><line x1="12" y1="16" x2="12.01" y2="16"></line></svg>',
            'warning': '<svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.46 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg>',
            'healthy': '<svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path><polyline points="22 4 12 14.01 9 11.01"></polyline></svg>',
            'exec_summary': '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"></path><polyline points="14 2 14 8 20 8"></polyline><line x1="16" y1="13" x2="8" y2="13"></line><line x1="16" y1="17" x2="8" y2="17"></line><line x1="10" y1="9" x2="8" y2="9"></line></svg>',
            'actions': '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>',
            'error_list': '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="15" y1="9" x2="9" y2="15"></line><line x1="9" y1="9" x2="15" y2="15"></line></svg>'
        }
        
        # Status styling based on filtered severity
        if filtered_anomalies_count == 0:
            status_color = "#10b981"  # Green
            status_indicator = "HEALTHY"
            severity_class = "success"
            header_icon = icons['healthy']
        elif filtered_anomalies_count <= 5:
            status_color = "#f59e0b"  # Yellow
            status_indicator = "WARNING"
            severity_class = "warning"
            header_icon = icons['warning']
        else:
            status_color = "#ef4444"  # Red
            status_indicator = "CRITICAL"
            severity_class = "danger"
            header_icon = icons['critical']
        
        return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sectors Guard Validation Alert</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
        
        * {{
            box-sizing: border-box;
        }}
        
        @media screen and (max-width: 640px) {{
            .email-container {{
                width: 100% !important;
                max-width: none !important;
                margin: 0 !important;
                border-radius: 0 !important;
            }}
            .email-header {{
                padding: 25px 20px !important;
            }}
            .email-header h1 {{
                font-size: 24px !important;
            }}
            .email-content {{
                padding: 30px 20px !important;
            }}
            .stats-card {{
                padding: 20px 15px !important;
                margin: 20px 0 !important;
            }}
            .anomaly-card {{
                padding: 15px !important;
                margin: 15px 0 !important;
            }}
            .email-footer {{
                padding: 25px 20px !important;
            }}
        }}
        
        .inter-font {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif;
        }}
        
        .inter-bold {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif;
            font-weight: 700;
        }}
        
        .status-badge {{
            padding: 12px 24px;
            border-radius: 50px;
            font-size: 14px;
            font-weight: 600;
            display: inline-block;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.1);
            border: none;
        }}
        
        .success {{ 
            background-color: #10b981;
            color: #ffffff;
        }}
        .warning {{ 
            background-color: #f59e0b;
            color: #ffffff;
        }}
        .danger {{ 
            background-color: #ef4444;
            color: #ffffff;
        }}
        
        .anomaly-card {{
            background: #fef2f2;
            border: 1px solid #fecaca;
            border-left: 5px solid #ef4444;
            padding: 24px;
            margin: 20px 0;
            border-radius: 12px;
            box-shadow: 0 4px 12px rgba(239, 68, 68, 0.1);
            transition: all 0.3s ease;
        }}
        
        .btn-primary {{
            background: #2563eb;
            color: #ffffff;
            padding: 12px 24px;
            border-radius: 8px;
            text-decoration: none;
            font-weight: 600;
            display: inline-block;
            box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
            transition: all 0.3s ease;
        }}
        
        .card-shadow {{
            box-shadow: 0 10px 25px rgba(0,0,0,0.1), 0 4px 10px rgba(0,0,0,0.05);
        }}
        
        .gradient-text {{
            background: linear-gradient(135deg, #1e3a8a, #3b82f6);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
    </style>
</head>
<body style="margin: 0; padding: 0; font-family: 'Inter', sans-serif; background-color: #f1f5f9; -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; min-height: 100vh;">
    <table role="presentation" style="width: 100%; margin: 0; padding: 0; background-color: #f1f5f9;" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td align="center" style="padding: 30px 20px;">
                <div class="email-container inter-font card-shadow" style="max-width: 920px; width: 100%; margin: 0 auto; background-color: #ffffff; border-radius: 16px; overflow: hidden;">
                    
                    <div class="email-header" style="background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 100%); padding: 40px 30px; text-align: center; position: relative; overflow: hidden;">
                         <div style="position: relative; z-index: 1;">
                            <!-- <div style="width: 80px; height: 80px; margin: 0 auto 20px; color: #ffffff;">
                                {header_icon}
                            </div> -->
                            <h1 class="inter-bold" style="color: #ffffff; margin: 0; font-size: 32px; font-weight: 700; text-shadow: 0 2px 4px rgba(0,0,0,0.2); line-height: 1.2; letter-spacing: -0.02em;">
                                DATA VALIDATION ALERT
                            </h1>
                            <p class="inter-font" style="color: rgba(255,255,255,0.9); margin: 16px 0 0 0; font-size: 18px; line-height: 1.4; font-weight: 400;">
                                Quality monitoring for <strong style="color: #ffffff; font-weight: 600;">{table_name}</strong>
                            </p>
                        </div>
                    </div>
                    
                    <div class="email-content" style="padding: 40px 35px; text-align: left;">
                        <div style="text-align: center; margin-bottom: 35px;">
                            <span class="status-badge {severity_class} inter-font">
                                {filtered_anomalies_count} Critical Issues Detected
                            </span>
                        </div>
                        
                        <p class="inter-font" style="color: #374151; font-size: 18px; line-height: 1.7; margin: 0 0 24px 0; font-weight: 400;">
                            Hello,
                        </p>
                        
                        <p class="inter-font" style="color: #6b7280; font-size: 16px; line-height: 1.7; margin: 0 0 32px 0; font-weight: 400;">
                            Our automated validation system has completed analysis of the <strong class="gradient-text" style="font-weight: 600;">{table_name}</strong> dataset and identified <strong style="color: #ef4444; font-weight: 600;">{filtered_anomalies_count}</strong> critical issues that require immediate attention.
                        </p>
                        
                        <div class="stats-card" style="background-color: #f8fafc; border: 1px solid #e5e7eb; padding: 32px; margin: 32px 0; border-radius: 16px; box-shadow: 0 4px 12px rgba(0,0,0,0.05);">
                            <div style="display: flex; align-items: center; margin-bottom: 24px;">
                                <div style="background-color: #3b82f6; color: #ffffff; border-radius: 12px; width: 48px; height: 48px; display: flex; align-items: center; justify-content: center; margin-right: 16px;">
                                    {icons['exec_summary']}
                                </div>
                                <h3 class="inter-bold" style="color: #111827; margin: 0; font-size: 22px; line-height: 1.3; font-weight: 700;">
                                    Executive Summary
                                </h3>
                            </div>
                            
                            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px;">
                                <div style="background: #ffffff; padding: 20px; border-radius: 12px; border: 1px solid #f3f4f6;">
                                    <div class="inter-font" style="color: #9ca3af; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">Dataset</div>
                                    <div class="inter-font" style="color: #111827; font-size: 16px; font-weight: 600; word-break: break-word;">{table_name}</div>
                                </div>
                                <div style="background: #ffffff; padding: 20px; border-radius: 12px; border: 1px solid #f3f4f6;">
                                    <div class="inter-font" style="color: #9ca3af; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">Status</div>
                                    <div class="inter-font" style="color: #111827; font-size: 16px; font-weight: 600; text-transform: capitalize;">{status}</div>
                                </div>
                                <div style="background: #ffffff; padding: 20px; border-radius: 12px; border: 1px solid #f3f4f6;">
                                    <div class="inter-font" style="color: #9ca3af; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">Total Records</div>
                                    <div class="inter-font" style="color: #111827; font-size: 16px; font-weight: 600;">{total_rows:,}</div>
                                </div>
                                <div style="background: #ffffff; padding: 20px; border-radius: 12px; border: 1px solid #f3f4f6;">
                                    <div class="inter-font" style="color: #9ca3af; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">Critical Issues</div>
                                    <div class="inter-font" style="color: #ef4444; font-size: 16px; font-weight: 700;">{filtered_anomalies_count}</div>
                                </div>
                                <div style="background: #ffffff; padding: 20px; border-radius: 12px; border: 1px solid #f3f4f6;">
                                    <div class="inter-font" style="color: #9ca3af; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">Validated</div>
                                    <div class="inter-font" style="color: #111827; font-size: 14px; font-weight: 500;">{datetime.fromisoformat(validation_timestamp.replace('Z', '+00:00')).strftime('%b %d, %Y') if validation_timestamp else 'N/A'}</div>
                                </div>
                                <div style="background: #ffffff; padding: 20px; border-radius: 12px; border: 1px solid #f3f4f6;">
                                    <div class="inter-font" style="color: #9ca3af; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">Validations</div>
                                    <div class="inter-font" style="color: #111827; font-size: 14px; font-weight: 500;">{len(validations_performed) if validations_performed else 'Standard'} checks</div>
                                </div>
                            </div>
                        </div>
                        
                        {self._build_anomalies_section(anomalies)}
                        
                            <div style="background-color: #eff6ff; border: 1px solid #dbeafe; padding: 28px; margin: 32px 0; border-radius: 16px;">
                            <div style="display: flex; align-items: center; margin-bottom: 20px;">
                                <div style="background-color: #10b981; color: #ffffff; border-radius: 10px; width: 40px; height: 40px; display: flex; align-items: center; justify-content: center; margin-right: 12px;">
                                    {icons['actions']}
                                </div>
                                <h3 class="inter-bold" style="color: #111827; margin: 0; font-size: 20px; font-weight: 700;">Recommended Actions</h3>
                            </div>
                            <div style="background: #ffffff; border-radius: 12px; padding: 24px; border: 1px solid #f3f4f6;">
                                <ol class="inter-font" style="color: #6b7280; font-size: 15px; line-height: 1.8; margin: 0; padding-left: 20px;">
                                    <li style="margin-bottom: 12px; padding-left: 8px;">
                                        <strong style="color: #374151; font-weight: 600;">Review Critical Issues:</strong> Examine the {filtered_anomalies_count} error-level validation issues in {table_name}
                                    </li>
                                    <li style="margin-bottom: 12px; padding-left: 8px;">
                                        <strong style="color: #374151; font-weight: 600;">Dashboard Analysis:</strong> Check the Sectors Guard dashboard for detailed trends and patterns
                                    </li>
                                    <li style="margin-bottom: 12px; padding-left: 8px;">
                                        <strong style="color: #374151; font-weight: 600;">Root Cause Investigation:</strong> Identify underlying causes of data quality degradation
                                    </li>
                                    <li style="margin-bottom: 12px; padding-left: 8px;">
                                        <strong style="color: #374151; font-weight: 600;">Implement Fixes:</strong> Apply corrective measures to prevent future occurrences
                                    </li>
                                    <li style="padding-left: 8px;">
                                        <strong style="color: #374151; font-weight: 600;">Monitor Progress:</strong> Track improvements in subsequent validation cycles
                                    </li>
                                </ol>
                            </div>
                        </div>
                        
                        <div style="text-align: center; margin: 40px 0 32px 0;">
                            <a href="https://sectors-guard.vercel.app/" class="btn-primary inter-font" style="background-color: #2563eb; color: #ffffff; padding: 16px 32px; border-radius: 12px; text-decoration: none; font-weight: 600; display: inline-block; box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3); font-size: 16px; letter-spacing: 0.25px;">
                                View Dashboard →
                            </a>
                        </div>
                    </div>
                    
                    <div class="email-footer" style="background-color: #111827; padding: 40px 30px; text-align: center; border-top: 1px solid #374151;">
                        <div style="max-width: 600px; margin: 0 auto;">
                            <div style="border-bottom: 1px solid #374151; padding-bottom: 24px; margin-bottom: 24px;">
                                <div style="display: flex; align-items: center; justify-content: center; margin-bottom: 12px;">
                                    <div style="background: #2563eb; border-radius: 12px; width: 48px; height: 48px; display: flex; align-items: center; justify-content: center; margin-right: 12px;">
                                        <span style="color: #ffffff; font-size: 14px; font-weight: 700; letter-spacing: 0.5px;">SG</span>
                                    </div>
                                    <h3 class="inter-bold" style="color: #ffffff; margin: 0; font-size: 24px; font-weight: 700; letter-spacing: -0.02em;">
                                        Sectors Guard
                                    </h3>
                                </div>
                                <p class="inter-font" style="color: #d1d5db; margin: 0; font-size: 16px; line-height: 1.5; font-weight: 400;">
                                    Enterprise Data Quality Monitoring
                                </p>
                            </div>
                            
                            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 24px;">
                                <div style="text-align: center;">
                                    <div style="color: #9ca3af; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">Need Help?</div>
                                    <div style="color: #e5e7eb; font-size: 14px; font-weight: 500;">Support Team Available</div>
                                </div>
                                <div style="text-align: center;">
                                    <div style="color: #9ca3af; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">Powered By</div>
                                    <div style="color: #e5e7eb; font-size: 14px; font-weight: 500;">Supertype AI</div>
                                </div>
                            </div>
                            
                            <div style="background: rgba(59, 130, 246, 0.1); border: 1px solid rgba(59, 130, 246, 0.2); border-radius: 12px; padding: 20px; margin-bottom: 24px;">
                                <p class="inter-font" style="color: #93c5fd; font-size: 13px; margin: 0; line-height: 1.6; font-weight: 400;">
                                    • This automated alert was generated by your data validation system<br>
                                    • Sent on {datetime.now().strftime('%B %d, %Y at %I:%M %p')} UTC<br>
                                    • Delivered within seconds of detection
                                </p>
                            </div>
                            
                            <p class="inter-font" style="color: #6b7280; font-size: 12px; margin: 0; line-height: 1.5; font-weight: 400;">
                                © 2025 Supertype. All rights reserved. | Data protection and quality assurance platform.
                            </p>
                        </div>
                    </div>
                </div>
            </td>
        </tr>
    </table>
</body>
</html>
        """
    
    def _build_anomalies_section(self, anomalies: List[Dict[str, Any]]) -> str:
        """Build the anomalies section of the email"""
        # --- Icon Definitions ---
        icons = {
            'healthy_check': '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path><polyline points="22 4 12 14.01 9 11.01"></polyline></svg>',
            'error_cross': '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg>',
            'error_list': '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="15" y1="9" x2="9" y2="15"></line><line x1="9" y1="9" x2="15" y2="15"></line></svg>'
        }
        
        # Filter to only show 'error' severity anomalies
        filtered_anomalies = [
            anomaly for anomaly in anomalies 
            if anomaly.get('severity', '').lower() == 'error'
        ]
        
        if not filtered_anomalies:
            return f"""
            <div style="background-color: #ecfdf5; border: 1px solid #d1fae5; border-left: 5px solid #10b981; padding: 28px; margin: 32px 0; border-radius: 16px;">
                <div style="display: flex; align-items: center; margin-bottom: 16px;">
                    <div style="background-color: #10b981; color: #ffffff; border-radius: 12px; width: 48px; height: 48px; display: flex; align-items: center; justify-content: center; margin-right: 16px;">
                        {icons['healthy_check']}
                    </div>
                    <h3 class="inter-bold" style="color: #065f46; margin: 0; font-size: 20px; font-weight: 700;">All Systems Healthy</h3>
                </div>
                <p class="inter-font" style="color: #047857; margin: 0; font-size: 16px; line-height: 1.6;">No critical issues detected in this validation cycle. Your data quality standards are being maintained successfully!</p>
            </div>
            """
        
        anomalies_html = f"""
        <div style="margin: 32px 0;">
            <div style="display: flex; align-items: center; margin-bottom: 24px;">
                <div style="background-color: #ef4444; color: #ffffff; border-radius: 12px; width: 48px; height: 48px; display: flex; align-items: center; justify-content: center; margin-right: 16px;">
                    {icons['error_list']}
                </div>
                <h3 class="inter-bold" style="color: #111827; margin: 0; font-size: 22px; font-weight: 700;">Critical Issues Detected</h3>
            </div>
        """
        
        for anomaly in filtered_anomalies[:8]:  # Limit to first 8 critical issues
            severity = anomaly.get('severity', 'error').lower()
            anomaly_type = anomaly.get('type', 'Unknown Issue').replace('_', ' ').title()
            message = anomaly.get('message', 'No details provided')
            
            anomaly_html = f"""
            <div style="background: #fef2f2; border: 1px solid #fecaca; border-left: 5px solid #ef4444; padding: 24px; margin: 20px 0; border-radius: 16px; box-shadow: 0 4px 12px rgba(239, 68, 68, 0.08);">
                <div style="display: flex; justify-content: between; align-items: flex-start; margin-bottom: 16px;">
                    <div style="display: flex; align-items: center; flex: 1;">
                        <div style="background-color: #ef4444; color: #ffffff; border-radius: 10px; width: 40px; height: 40px; display: flex; align-items: center; justify-content: center; margin-right: 16px; box-shadow: 0 2px 6px rgba(239, 68, 68, 0.3);">
                            {icons['error_cross']}
                        </div>
                        <div>
                            <h4 class="inter-bold" style="color: #111827; margin: 0 0 4px 0; font-size: 18px; font-weight: 700; line-height: 1.3;">
                                {anomaly_type}
                            </h4>
                            <span style="background-color: #dc2626; color: #ffffff; padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;">
                                CRITICAL
                            </span>
                        </div>
                    </div>
                </div>
                
                <div style="background: #ffffff; border-radius: 12px; padding: 20px; border: 1px solid #f3f4f6;">
                    <div style="margin-bottom: 16px;">
                        <div class="inter-font" style="color: #6b7280; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;">Issue Description</div>
                        <p class="inter-font" style="color: #374151; margin: 0; font-size: 15px; line-height: 1.6; font-weight: 400;">
                            {message}
                        </p>
                    </div>
                    
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 16px;">"""
            
            # Add symbol information if available
            if anomaly.get('symbol'):
                anomaly_html += f"""
                        <div>
                            <div class="inter-font" style="color: #6b7280; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px;">Symbol</div>
                            <span class="inter-font" style="background-color: #dbeafe; color: #1e40af; padding: 6px 12px; border-radius: 8px; font-weight: 600; font-size: 13px; display: inline-block;">
                                {anomaly.get('symbol')}
                            </span>
                        </div>"""
            
            # Add severity badge
            anomaly_html += f"""
                        <div>
                            <div class="inter-font" style="color: #6b7280; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px;">Severity</div>
                            <span class="inter-font" style="background-color: #ef4444; color: #ffffff; padding: 6px 12px; border-radius: 8px; font-weight: 600; font-size: 13px; text-transform: uppercase; display: inline-block;">
                                {severity}
                            </span>
                        </div>"""
            
            # Add period/periods information if available (support both 'period' and 'periods')
            period_info = anomaly.get('periods') or anomaly.get('period')
            if period_info:
                anomaly_html += f"""
                <p class="inter-font" style="color: #555; margin: 0 0 8px 0; font-size: 14px;">
                    <strong>Periods:</strong> <span style="background-color: #f3e5f5; padding: 2px 6px; border-radius: 4px; font-weight: 600; color: #7b1fa2;">{period_info}</span>
                </p>
                """
            
            # Add date information if available
            if anomaly.get('date'):
                anomaly_html += f"""
                        <div>
                            <div class="inter-font" style="color: #6b7280; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px;">Date</div>
                            <div class="inter-font" style="color: #374151; font-size: 14px; font-weight: 500;">{anomaly.get('date')}</div>
                        </div>"""
            
            if anomaly.get('column'):
                anomaly_html += f"""
                        <div>
                            <div class="inter-font" style="color: #6b7280; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px;">Column</div>
                            <div class="inter-font" style="color: #374151; font-size: 14px; font-weight: 500; font-family: 'Monaco', 'Menlo', monospace;">{anomaly.get('column')}</div>
                        </div>"""
            
            if anomaly.get('count'):
                anomaly_html += f"""
                        <div>
                            <div class="inter-font" style="color: #6b7280; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px;">Affected Records</div>
                            <div class="inter-font" style="color: #ef4444; font-size: 14px; font-weight: 700;">{anomaly.get('count'):,}</div>
                        </div>"""
            
            # Close the grid and card
            anomaly_html += """
                    </div>
                </div>
            </div>
            """
            anomalies_html += anomaly_html
        
        # Show summary if more issues exist
        if len(filtered_anomalies) > 8:
            anomalies_html += f"""
            <div style="background-color: #fefce8; border: 1px solid #fde68a; border-left: 5px solid #f59e0b; padding: 20px; margin: 20px 0; border-radius: 12px; text-align: center;">
                <p class="inter-font" style="color: #92400e; margin: 0; font-size: 15px; font-weight: 500;">
                    <strong>{len(filtered_anomalies) - 8} additional critical issues</strong> detected.<br>
                    View the complete analysis in your dashboard for full details.
                </p>
            </div>
            """
        
        anomalies_html += """
        </div>
        """
        
        return anomalies_html

    def _build_summary_email_html(self, summary_data: Dict[str, Any]) -> str:
        """Return the HTML email body for daily summary."""
        total_validations = summary_data.get('total_validations', 0)
        total_anomalies = summary_data.get('total_anomalies', 0)
        tables_validated = summary_data.get('tables_validated', [])
        top_issues = summary_data.get('top_issues', [])
        
        return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sectors Guard Daily Summary</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
        
        .inter-font {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
        }}
        
        .inter-bold {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            font-weight: 700;
        }}
    </style>
</head>
<body style="margin: 0; padding: 0; font-family: 'Inter', sans-serif; background-color: #f4f6f9;">
    <table role="presentation" style="width: 100%; margin: 0; padding: 0; background-color: #f4f6f9;" cellpadding="0" cellspacing="0" border="0">
        <tr>
            <td align="center" style="padding: 20px;">
                <div class="email-container inter-font" style="max-width: 900px; width: 100%; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 8px 24px rgba(0,0,0,0.1);">
                    
                    <div class="email-header" style="background: #10b981; padding: 30px 25px; text-align: center;">
                        <h1 class="inter-bold" style="color: #ffffff; margin: 0; font-size: 28px; font-weight: 700; text-shadow: 0 2px 4px rgba(0,0,0,0.2);">
                            Daily Validation Summary
                        </h1>
                        <p class="inter-font" style="color: #ffffff; margin: 12px 0 0 0; font-size: 16px; opacity: 0.95;">
                            {datetime.now().strftime('%B %d, %Y')} - Data Quality Report
                        </p>
                    </div>
                    
                    <div class="email-content" style="padding: 35px 30px;">
                        
                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 25px 0;">
                            <div style="background-color: #f0f9ff; padding: 20px; border-radius: 10px; text-align: center; border-left: 4px solid #3b82f6;">
                                <h3 class="inter-bold" style="color: #1e40af; margin: 0 0 5px 0; font-size: 24px;">{total_validations}</h3>
                                <p class="inter-font" style="color: #1e40af; margin: 0; font-size: 14px; font-weight: 500;">Total Validations</p>
                            </div>
                            <div style="background-color: #fef3c7; padding: 20px; border-radius: 10px; text-align: center; border-left: 4px solid #f59e0b;">
                                <h3 class="inter-bold" style="color: #92400e; margin: 0 0 5px 0; font-size: 24px;">{total_anomalies}</h3>
                                <p class="inter-font" style="color: #92400e; margin: 0; font-size: 14px; font-weight: 500;">Issues Detected</p>
                            </div>
                            <div style="background-color: #ecfdf5; padding: 20px; border-radius: 10px; text-align: center; border-left: 4px solid #10b981;">
                                <h3 class="inter-bold" style="color: #047857; margin: 0 0 5px 0; font-size: 24px;">{len(tables_validated)}</h3>
                                <p class="inter-font" style="color: #047857; margin: 0; font-size: 14px; font-weight: 500;">Tables Monitored</p>
                            </div>
                        </div>
                        
                        {self._build_tables_summary(tables_validated)}
                        
                        {self._build_top_issues_summary(top_issues)}
                        
                    </div>
                    
                    <div class="email-footer" style="background-color: #1f2937; padding: 30px 25px; text-align: center;">
                        <h3 class="inter-bold" style="color: #10b981; margin: 0 0 10px 0; font-size: 20px;">
                            Sectors Guard
                        </h3>
                        <p class="inter-font" style="color: #d1d5db; margin: 0 0 15px 0; font-size: 14px;">
                            Your trusted data quality guardian
                        </p>
                        <p class="inter-font" style="color: #9ca3af; font-size: 12px; margin: 0;">
                            © 2025 Supertype. All rights reserved.
                        </p>
                    </div>
                </div>
            </td>
        </tr>
    </table>
</body>
</html>
        """
    
    def _build_tables_summary(self, tables_validated: List[str]) -> str:
        """Build the tables summary section"""
        if not tables_validated:
            return ""
        
        tables_html = """
        <div style="margin: 30px 0;">
            <h3 class="inter-bold" style="color: #1e3a8a; margin: 0 0 15px 0; font-size: 18px;">Tables Validated Today</h3>
            <div style="background-color: #f8fafc; padding: 20px; border-radius: 10px; border-left: 4px solid #3b82f6;">
        """
        
        for i, table in enumerate(tables_validated):
            tables_html += f"""
                <span class="inter-font" style="display: inline-block; background-color: #e0e7ff; color: #1e40af; padding: 6px 12px; margin: 4px; border-radius: 15px; font-size: 13px; font-weight: 500;">
                    {table}
                </span>
            """
        
        tables_html += """
            </div>
        </div>
        """
        
        return tables_html
    
    def _build_top_issues_summary(self, top_issues: List[Dict[str, Any]]) -> str:
        """Build the top issues summary section"""
        if not top_issues:
            return """
            <div style="margin: 30px 0;">
                <h3 class="inter-bold" style="color: #059669; margin: 0 0 15px 0; font-size: 18px;">No Critical Issues</h3>
                <div style="background-color: #ecfdf5; padding: 20px; border-radius: 10px; border-left: 4px solid #10b981;">
                    <p class="inter-font" style="color: #047857; margin: 0; font-size: 15px;">
                        Excellent! All validations passed without critical issues today.
                    </p>
                </div>
            </div>
            """
        
        issues_html = """
        <div style="margin: 30px 0;">
            <h3 class="inter-bold" style="color: #dc3545; margin: 0 0 15px 0; font-size: 18px;">Top Issues Detected</h3>
        """
        
        for issue in top_issues[:5]:  # Show top 5 issues
            issues_html += f"""
            <div style="background-color: #fff5f5; padding: 15px; margin: 10px 0; border-radius: 8px; border-left: 4px solid #ef4444;">
                <p class="inter-font" style="color: #dc2626; margin: 0 0 5px 0; font-size: 14px; font-weight: 600;">
                    {issue.get('type', 'Unknown Issue').replace('_', ' ').title()}
                </p>
                <p class="inter-font" style="color: #991b1b; margin: 0; font-size: 13px;">
                    Table: {issue.get('table', 'N/A')} | Count: {issue.get('count', 0)}
                </p>
            </div>
            """
        
        issues_html += "</div>"
        return issues_html

    async def _send_validation_email_smtp_fallback(self, recipient_email: str, table_name: str, 
                                                  validation_results: Dict[str, Any]) -> bool:
        """Fallback method using SMTP"""
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        logger.info(f"Using SMTP fallback for validation email to {recipient_email}")
        
        try:
            anomalies_count = validation_results.get('anomalies_count', 0)
            subject = f"Sectors Guard Alert: {table_name} - {anomalies_count} validation issues detected"
            
            # Format sender with display name
            sender_formatted = format_email_with_display_name(self.smtp_username or self.default_from_email)
            
            msg = MIMEMultipart()
            msg['Subject'] = subject
            msg['From'] = sender_formatted
            msg['To'] = recipient_email
            
            # HTML body
            html_content = self._build_validation_email_html(table_name, validation_results)
            html_body = MIMEText(html_content, 'html')
            msg.attach(html_body)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_username, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"SMTP validation email sent successfully to {recipient_email}")
            return True
            
        except Exception as e:
            logger.error(f"SMTP validation email failed for {recipient_email}: {e}")
            return False

    async def _send_summary_email_smtp_fallback(self, recipient_email: str, summary_data: Dict[str, Any]) -> bool:
        """Fallback method using SMTP for summary emails"""
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        logger.info(f"Using SMTP fallback for summary email to {recipient_email}")
        
        try:
            subject = f"Sectors Guard Daily Summary - {datetime.now().strftime('%B %d, %Y')}"
            
            # Format sender with display name
            sender_formatted = format_email_with_display_name(self.smtp_username or self.default_from_email)
            
            msg = MIMEMultipart()
            msg['Subject'] = subject
            msg['From'] = sender_formatted
            msg['To'] = recipient_email
            
            # HTML body
            html_content = self._build_summary_email_html(summary_data)
            html_body = MIMEText(html_content, 'html')
            msg.attach(html_body)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_username, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"SMTP summary email sent successfully to {recipient_email}")
            return True
            
        except Exception as e:
            logger.error(f"SMTP summary email failed for {recipient_email}: {e}")
            return False

    async def _get_email_recipients(self, table_name: str) -> List[str]:
        """Get email recipients for a specific table from validation_configs table"""
        try:
            # Try to get table-specific recipients from validation_configs table
            response = self.supabase.table("validation_configs").select("email_recipients").eq("table_name", table_name).single().execute()
            
            if response.data and response.data.get("email_recipients"):
                recipients = response.data["email_recipients"]
                # Ensure it's a list and filter out any empty strings
                if isinstance(recipients, list):
                    return [email for email in recipients if email and isinstance(email, str)]
            
            # Fallback to default recipients if table-specific config is missing or empty
            logger.info(f"No specific recipients found for {table_name}, using default recipients.")
            return self._get_default_recipients()
            
        except Exception as e:
            logger.error(f"Error getting email recipients for {table_name}: {e}")
            return self._get_default_recipients()
    
    def _get_default_recipients(self) -> List[str]:
        """Get default email recipients from environment variables"""
        recipients_str = os.getenv("DEFAULT_EMAIL_RECIPIENTS", "")
        if recipients_str:
            return [email.strip() for email in recipients_str.split(",") if email.strip()]
        return []

# Global email service instance
validation_email_service = ValidationEmailService()