"""
Base models and mixins for the Enterprise Data Engineering Platform.
Provides common functionality across all domain models.
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, event
from sqlalchemy.orm import declarative_base, declared_attr
from sqlalchemy.sql import func

Base = declarative_base()


class TimestampMixin:
    """Mixin that adds created_at and updated_at timestamps."""
    
    @declared_attr
    def created_at(cls):
        return Column(DateTime, default=func.now(), nullable=False)
    
    @declared_attr
    def updated_at(cls):
        return Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)


class AuditMixin:
    """Mixin for audit trail functionality (SOX/HIPAA compliance)."""
    
    @declared_attr
    def created_by(cls):
        return Column(String(100), nullable=True)
    
    @declared_attr
    def updated_by(cls):
        return Column(String(100), nullable=True)
    
    @declared_attr
    def is_deleted(cls):
        return Column(Boolean, default=False, nullable=False)
    
    @declared_attr
    def deleted_at(cls):
        return Column(DateTime, nullable=True)
    
    @declared_attr
    def deleted_by(cls):
        return Column(String(100), nullable=True)
    
    @declared_attr
    def version(cls):
        return Column(Integer, default=1, nullable=False)
    
    @declared_attr
    def change_reason(cls):
        return Column(Text, nullable=True)


class SoftDeleteMixin:
    """Mixin for soft delete functionality."""
    
    @declared_attr
    def is_active(cls):
        return Column(Boolean, default=True, nullable=False)
    
    def soft_delete(self, deleted_by: Optional[str] = None):
        """Mark record as deleted without removing from database."""
        self.is_active = False
        if hasattr(self, 'is_deleted'):
            self.is_deleted = True
        if hasattr(self, 'deleted_at'):
            self.deleted_at = datetime.utcnow()
        if hasattr(self, 'deleted_by') and deleted_by:
            self.deleted_by = deleted_by


class DataLineageMixin:
    """Mixin for tracking data lineage (important for data engineering)."""
    
    @declared_attr
    def source_system(cls):
        return Column(String(100), nullable=True)
    
    @declared_attr
    def source_file(cls):
        return Column(String(500), nullable=True)
    
    @declared_attr
    def ingestion_timestamp(cls):
        return Column(DateTime, default=func.now(), nullable=False)
    
    @declared_attr
    def etl_job_id(cls):
        return Column(String(100), nullable=True)
    
    @declared_attr
    def data_quality_score(cls):
        return Column(Integer, nullable=True)  # 0-100 score


class EncryptionMixin:
    """Mixin for PII/sensitive data encryption markers."""
    
    @declared_attr
    def encryption_key_id(cls):
        return Column(String(100), nullable=True)
    
    @declared_attr
    def is_encrypted(cls):
        return Column(Boolean, default=False, nullable=False)
