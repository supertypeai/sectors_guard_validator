"""
Database models for validation results and configurations
"""

from sqlalchemy import Column, Integer, String, DateTime, JSON, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class ValidationResult(Base):
    __tablename__ = "validation_results"
    
    id = Column(Integer, primary_key=True, index=True)
    table_name = Column(String, nullable=False)
    validation_type = Column(String, nullable=False)
    status = Column(String, nullable=False)  # success, warning, error
    anomalies_count = Column(Integer, default=0)
    details = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    email_sent = Column(Boolean, default=False)

class ValidationConfig(Base):
    __tablename__ = "validation_configs"
    
    id = Column(Integer, primary_key=True, index=True)
    table_name = Column(String, nullable=False, unique=True)
    validation_rules = Column(JSON, nullable=False)
    email_recipients = Column(JSON)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class EmailLog(Base):
    __tablename__ = "email_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    validation_result_id = Column(Integer, nullable=False)
    recipients = Column(JSON)
    subject = Column(String)
    body = Column(Text)
    sent_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String)  # sent, failed
