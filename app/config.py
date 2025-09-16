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
    
    # AWS SES settings
    aws_access_key_id: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    default_from_email: Optional[str] = os.getenv("DEFAULT_FROM_EMAIL")
    default_from_name: str = os.getenv("DEFAULT_FROM_NAME", "Sectors Guard")
    
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
    frontend_url: str = os.getenv("FRONTEND_URL", "http://localhost:3000")

    password: str = os.getenv("PASSWORD")

    # Additional CORS origins (comma-separated) or defaults
    cors_origins: List[str] = []
    
    def get_cors_origins(self) -> List[str]:
        """Get properly configured CORS origins including production defaults"""
        default_origins = [
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "https://sectors-guard.vercel.app"
        ]
        
        if os.getenv("CORS_ORIGINS"):
            env_origins_raw = os.getenv("CORS_ORIGINS", "")
            
            # Handle cases where the env var might be a string representation of a list
            # e.g., '["url1", "url2"]' or "'url1','url2'"
            cleaned_origins = env_origins_raw.replace('[', '').replace(']', '').replace('"', '').replace("'", "")
            
            env_origins = [
                origin.strip() for origin in cleaned_origins.split(",")
                if origin.strip()
            ]
            # Combine env origins with defaults, remove duplicates
            all_origins = env_origins + default_origins
            return list(dict.fromkeys(all_origins))
        else:
            # Ensure frontend_url is included
            if self.frontend_url and self.frontend_url not in default_origins:
                default_origins.insert(0, self.frontend_url)
            return default_origins
    
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
            
        # Initialize cors_origins using the method
        self.cors_origins = self.get_cors_origins()
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# Global settings instance
settings = Settings()
