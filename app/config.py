import os
import json
from pydantic_settings import BaseSettings
from pydantic_settings.sources import EnvSettingsSource
from typing import List, Optional

class _GracefulEnvSettingsSource(EnvSettingsSource):
    def prepare_field_value(self, field_name, field, value, value_is_complex):
        # Handle empty strings for non-complex fields (e.g. smtp_port: int = 587)
        if not value_is_complex and isinstance(value, str) and value.strip() == '':
            return None  # let pydantic use the field default
        return super().prepare_field_value(field_name, field, value, value_is_complex)

    def decode_complex_value(self, field_name, field, value):
        try:
            return super().decode_complex_value(field_name, field, value)
        except (json.JSONDecodeError, ValueError):
            # value is a non-JSON string: try comma-separated splitting
            if isinstance(value, str):
                stripped = value.strip()
                if not stripped:
                    return []
                return [item.strip() for item in stripped.split(",") if item.strip()]
            return value


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
    smtp_port: int = 587
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
    validation_timeout_seconds: int = 420  # 7 minutes
    
    # API settings
    api_host: str = "0.0.0.0"
    api_port: int = int(os.getenv("PORT", "8000"))
    debug: bool = os.getenv("DEBUG", "False").lower() == "true"
    
    # CORS settings
    frontend_url: str = os.getenv("FRONTEND_URL", "http://localhost:3000")

    password: Optional[str] = os.getenv("PASSWORD")

    # Additional CORS origins (comma-separated) or defaults
    cors_origins: List[str] = []

    # Backend auth token for protected endpoints
    backend_api_token: Optional[str] = os.getenv("BACKEND_API_TOKEN")

    # Optional Google Sheet CSV URL for backend fetch
    gsheet_csv_url: Optional[str] = os.getenv("GSHEET_CSV_URL")

    # ── Use our graceful env source instead of the default one ────────────
    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings):
        return (
            init_settings,
            _GracefulEnvSettingsSource(settings_cls),
            dotenv_settings,
            file_secret_settings,
        )

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
        
        # Parse smtp_port from environment variables, handling empty strings safely
        smtp_port_env = os.getenv("SMTP_PORT")
        if smtp_port_env:
            self.smtp_port = int(smtp_port_env)
        
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
