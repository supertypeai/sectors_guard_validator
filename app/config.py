"""
Configuration settings for the IDX Data Validation application
"""

import os
from pydantic_settings import BaseSettings
from typing import List, Optional

class Settings(BaseSettings):
    # Database settings
    supabase_url: str = os.getenv("SUPABASE_URL", "")
    supabase_key: str = os.getenv("SUPABASE_KEY", "")
    
    # JWT settings
    jwt_secret: str = os.getenv("JWT_SECRET", "default-secret-key")
    jwt_algorithm: str = "HS256"
    jwt_expiration_hours: int = 24
    
    # Email settings
    smtp_server: str = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port: int = int(os.getenv("SMTP_PORT", "587"))
    smtp_username: Optional[str] = os.getenv("SMTP_USERNAME")
    smtp_password: Optional[str] = os.getenv("SMTP_PASSWORD")
    from_email: Optional[str] = os.getenv("FROM_EMAIL")
    
    # Default email recipients
    default_email_recipients: List[str] = []
    daily_summary_recipients: List[str] = []
    
    # Validation settings
    default_error_threshold: int = 5
    validation_timeout_seconds: int = 300  # 5 minutes
    
    # API settings
    api_host: str = "0.0.0.0"
    api_port: int = int(os.getenv("PORT", "8000"))
    debug: bool = os.getenv("DEBUG", "False").lower() == "true"
    
    # CORS settings
    cors_origins: List[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Parse email recipients from environment variables
        if os.getenv("DEFAULT_EMAIL_RECIPIENTS"):
            self.default_email_recipients = [
                email.strip() for email in os.getenv("DEFAULT_EMAIL_RECIPIENTS", "").split(",")
                if email.strip()
            ]
        
        if os.getenv("DAILY_SUMMARY_RECIPIENTS"):
            self.daily_summary_recipients = [
                email.strip() for email in os.getenv("DAILY_SUMMARY_RECIPIENTS", "").split(",")
                if email.strip()
            ]
        
        # Set from_email to smtp_username if not specified
        if not self.from_email and self.smtp_username:
            self.from_email = self.smtp_username
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# Global settings instance
settings = Settings()
