"""
Database connection and configuration
"""

import os
from supabase import create_client, Client
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# SQLAlchemy configuration for direct database access
DATABASE_URL = f"postgresql://postgres:{os.getenv('DB_PASSWORD', '')}@{SUPABASE_URL.replace('https://', '').replace('.supabase.co', '')}.supabase.co:5432/postgres"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
metadata = MetaData()

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_database():
    """Initialize database connection"""
    try:
        # Test Supabase connection
        result = supabase.table('idx_dividend').select("*").limit(1).execute()
        print("✅ Supabase connection successful")
        
        # Check if validation tables exist
        try:
            supabase.table('validation_results').select("*").limit(1).execute()
            print("✅ Validation tables exist")
        except Exception as e:
            print(f"⚠️  Validation tables missing: {e}")
            print("💡 Run 'python init_database.py' to create required tables")
        
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def get_supabase_client() -> Client:
    """Get Supabase client instance"""
    return supabase
