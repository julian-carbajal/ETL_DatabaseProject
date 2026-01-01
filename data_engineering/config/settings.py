"""
Configuration settings for the Enterprise Data Engineering Platform.
Supports Health, Tech, Finance, and University domains.
"""

import os
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Any

# Base paths
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data_engineering" / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
LOGS_DIR = BASE_DIR / "data_engineering" / "logs"

# Create directories if they don't exist
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, WAREHOUSE_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)


@dataclass
class DatabaseConfig:
    """Database configuration for different environments."""
    db_type: str = "sqlite"  # sqlite, postgresql, mysql
    host: str = "localhost"
    port: int = 5432
    database: str = str(DATA_DIR / "enterprise_dw.db")
    user: str = "data_engineer"
    password: str = ""
    
    @property
    def connection_string(self) -> str:
        if self.db_type == "sqlite":
            return f"sqlite:///{self.database}"
        elif self.db_type == "postgresql":
            return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.db_type == "mysql":
            return f"mysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return f"sqlite:///{self.database}"


@dataclass
class ETLConfig:
    """ETL pipeline configuration."""
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay_seconds: int = 5
    parallel_workers: int = 4
    enable_data_quality_checks: bool = True
    enable_logging: bool = True
    log_level: str = "INFO"


@dataclass
class DomainConfig:
    """Domain-specific configuration."""
    # Health domain
    health_data_retention_days: int = 2555  # 7 years for HIPAA
    health_anonymize_pii: bool = True
    
    # Finance domain
    finance_decimal_precision: int = 4
    finance_audit_enabled: bool = True
    finance_sox_compliance: bool = True
    
    # Tech domain
    tech_metrics_interval_seconds: int = 60
    tech_log_retention_days: int = 90
    
    # University domain
    university_academic_year_start_month: int = 9
    university_grading_scale: str = "4.0"


# Global configuration instances
DB_CONFIG = DatabaseConfig()
ETL_CONFIG = ETLConfig()
DOMAIN_CONFIG = DomainConfig()


# Data quality thresholds
DATA_QUALITY_THRESHOLDS: Dict[str, Any] = {
    "completeness_threshold": 0.95,  # 95% non-null required
    "uniqueness_threshold": 1.0,     # 100% unique for PKs
    "validity_threshold": 0.98,      # 98% valid values
    "timeliness_hours": 24,          # Data should be < 24 hours old
    "consistency_threshold": 0.99,   # 99% cross-reference match
}


# Domain-specific validation rules
VALIDATION_RULES = {
    "health": {
        "patient_id": {"type": "string", "pattern": r"^PAT\d{8}$"},
        "ssn": {"type": "string", "pattern": r"^\d{3}-\d{2}-\d{4}$", "pii": True},
        "blood_pressure_systolic": {"type": "int", "min": 60, "max": 250},
        "blood_pressure_diastolic": {"type": "int", "min": 40, "max": 150},
        "heart_rate": {"type": "int", "min": 30, "max": 220},
    },
    "finance": {
        "account_number": {"type": "string", "pattern": r"^ACC\d{10}$"},
        "amount": {"type": "decimal", "min": -1e12, "max": 1e12},
        "currency": {"type": "string", "enum": ["USD", "EUR", "GBP", "JPY", "CNY"]},
        "transaction_type": {"type": "string", "enum": ["CREDIT", "DEBIT", "TRANSFER"]},
    },
    "tech": {
        "server_id": {"type": "string", "pattern": r"^SRV-[A-Z]{2}-\d{4}$"},
        "cpu_usage": {"type": "float", "min": 0, "max": 100},
        "memory_usage": {"type": "float", "min": 0, "max": 100},
        "response_time_ms": {"type": "float", "min": 0, "max": 60000},
    },
    "university": {
        "student_id": {"type": "string", "pattern": r"^STU\d{8}$"},
        "gpa": {"type": "float", "min": 0.0, "max": 4.0},
        "credits": {"type": "int", "min": 0, "max": 200},
        "enrollment_status": {"type": "string", "enum": ["ACTIVE", "GRADUATED", "SUSPENDED", "WITHDRAWN"]},
    },
}
