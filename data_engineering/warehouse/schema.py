"""
Data Warehouse Star Schema Implementation
==========================================

Implements dimensional modeling with star schema patterns.
Includes fact tables, dimension tables, and slowly changing dimensions.
"""

from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import logging

logger = logging.getLogger(__name__)


class SCDType(Enum):
    """Slowly Changing Dimension types."""
    TYPE_0 = 0  # Fixed - no changes allowed
    TYPE_1 = 1  # Overwrite - no history
    TYPE_2 = 2  # Add new row - full history
    TYPE_3 = 3  # Add new column - limited history


@dataclass
class Column:
    """Column definition for warehouse tables."""
    name: str
    data_type: str  # string, int, float, date, datetime, boolean
    nullable: bool = True
    primary_key: bool = False
    foreign_key: Optional[str] = None  # Reference to dimension table
    default: Any = None
    scd_type: SCDType = SCDType.TYPE_1


@dataclass
class DimensionTable:
    """
    Dimension table in a star schema.
    Stores descriptive attributes for analysis.
    """
    name: str
    columns: List[Column]
    surrogate_key: str = "sk"
    natural_key: List[str] = field(default_factory=list)
    scd_type: SCDType = SCDType.TYPE_1
    
    # SCD Type 2 tracking columns
    effective_date_column: str = "effective_date"
    expiration_date_column: str = "expiration_date"
    is_current_column: str = "is_current"
    
    def __post_init__(self):
        # Add surrogate key if not present
        sk_exists = any(c.name == self.surrogate_key for c in self.columns)
        if not sk_exists:
            self.columns.insert(0, Column(
                name=self.surrogate_key,
                data_type="int",
                nullable=False,
                primary_key=True,
            ))
        
        # Add SCD Type 2 columns if needed
        if self.scd_type == SCDType.TYPE_2:
            self._add_scd2_columns()
    
    def _add_scd2_columns(self):
        """Add SCD Type 2 tracking columns."""
        scd_columns = [
            Column(self.effective_date_column, "date", nullable=False),
            Column(self.expiration_date_column, "date", nullable=True),
            Column(self.is_current_column, "boolean", nullable=False, default=True),
        ]
        for col in scd_columns:
            if not any(c.name == col.name for c in self.columns):
                self.columns.append(col)
    
    def get_column_names(self) -> List[str]:
        """Get list of column names."""
        return [c.name for c in self.columns]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "type": "dimension",
            "surrogate_key": self.surrogate_key,
            "natural_key": self.natural_key,
            "scd_type": self.scd_type.value,
            "columns": [
                {
                    "name": c.name,
                    "data_type": c.data_type,
                    "nullable": c.nullable,
                    "primary_key": c.primary_key,
                }
                for c in self.columns
            ],
        }


@dataclass
class FactTable:
    """
    Fact table in a star schema.
    Stores measurable, quantitative data for analysis.
    """
    name: str
    columns: List[Column]
    grain: str  # Description of what each row represents
    dimension_keys: List[str] = field(default_factory=list)  # Foreign keys to dimensions
    measures: List[str] = field(default_factory=list)  # Numeric measure columns
    degenerate_dimensions: List[str] = field(default_factory=list)  # Dimensions stored in fact
    
    def __post_init__(self):
        # Identify measures and dimension keys from columns
        if not self.measures:
            self.measures = [
                c.name for c in self.columns
                if c.data_type in ["int", "float"] and not c.foreign_key
            ]
        
        if not self.dimension_keys:
            self.dimension_keys = [
                c.name for c in self.columns
                if c.foreign_key
            ]
    
    def get_column_names(self) -> List[str]:
        """Get list of column names."""
        return [c.name for c in self.columns]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "type": "fact",
            "grain": self.grain,
            "dimension_keys": self.dimension_keys,
            "measures": self.measures,
            "degenerate_dimensions": self.degenerate_dimensions,
            "columns": [
                {
                    "name": c.name,
                    "data_type": c.data_type,
                    "nullable": c.nullable,
                    "foreign_key": c.foreign_key,
                }
                for c in self.columns
            ],
        }


@dataclass
class StarSchema:
    """
    Complete star schema with fact and dimension tables.
    """
    name: str
    fact_table: FactTable
    dimensions: Dict[str, DimensionTable] = field(default_factory=dict)
    
    def add_dimension(self, dimension: DimensionTable):
        """Add a dimension table to the schema."""
        self.dimensions[dimension.name] = dimension
    
    def get_dimension(self, name: str) -> Optional[DimensionTable]:
        """Get a dimension table by name."""
        return self.dimensions.get(name)
    
    def validate(self) -> List[str]:
        """Validate schema integrity."""
        errors = []
        
        # Check that all fact table foreign keys reference existing dimensions
        for fk in self.fact_table.dimension_keys:
            # Extract dimension name from foreign key (e.g., "patient_sk" -> "patient")
            dim_name = fk.replace("_sk", "").replace("_key", "")
            if not any(dim_name in d.name.lower() for d in self.dimensions.values()):
                errors.append(f"Fact table references unknown dimension: {fk}")
        
        return errors
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "fact_table": self.fact_table.to_dict(),
            "dimensions": {
                name: dim.to_dict()
                for name, dim in self.dimensions.items()
            },
        }
    
    def generate_ddl(self, dialect: str = "postgresql") -> str:
        """Generate DDL statements for the schema."""
        ddl = []
        
        type_mapping = {
            "postgresql": {
                "string": "VARCHAR(255)",
                "int": "INTEGER",
                "float": "NUMERIC(18,4)",
                "date": "DATE",
                "datetime": "TIMESTAMP",
                "boolean": "BOOLEAN",
            },
            "mysql": {
                "string": "VARCHAR(255)",
                "int": "INT",
                "float": "DECIMAL(18,4)",
                "date": "DATE",
                "datetime": "DATETIME",
                "boolean": "TINYINT(1)",
            },
        }
        
        types = type_mapping.get(dialect, type_mapping["postgresql"])
        
        # Generate dimension tables first
        for dim in self.dimensions.values():
            columns = []
            for col in dim.columns:
                col_type = types.get(col.data_type, "VARCHAR(255)")
                nullable = "" if col.nullable else " NOT NULL"
                pk = " PRIMARY KEY" if col.primary_key else ""
                columns.append(f"    {col.name} {col_type}{nullable}{pk}")
            
            ddl.append(f"CREATE TABLE {dim.name} (\n" + ",\n".join(columns) + "\n);")
        
        # Generate fact table
        columns = []
        for col in self.fact_table.columns:
            col_type = types.get(col.data_type, "VARCHAR(255)")
            nullable = "" if col.nullable else " NOT NULL"
            columns.append(f"    {col.name} {col_type}{nullable}")
        
        ddl.append(f"CREATE TABLE {self.fact_table.name} (\n" + ",\n".join(columns) + "\n);")
        
        return "\n\n".join(ddl)


# =============================================================================
# Common Dimension Tables
# =============================================================================

class DateDimension:
    """
    Standard date dimension generator.
    Pre-populates date attributes for analysis.
    """
    
    @staticmethod
    def generate(start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Generate date dimension records."""
        records = []
        current = start_date
        sk = 1
        
        while current <= end_date:
            record = {
                "date_sk": sk,
                "date_actual": current.isoformat(),
                "day_of_week": current.weekday(),
                "day_name": current.strftime("%A"),
                "day_of_month": current.day,
                "day_of_year": current.timetuple().tm_yday,
                "week_of_year": current.isocalendar()[1],
                "month_number": current.month,
                "month_name": current.strftime("%B"),
                "month_name_short": current.strftime("%b"),
                "quarter": (current.month - 1) // 3 + 1,
                "year": current.year,
                "year_month": current.strftime("%Y-%m"),
                "year_quarter": f"{current.year}-Q{(current.month - 1) // 3 + 1}",
                "is_weekend": current.weekday() >= 5,
                "is_holiday": False,  # Would need holiday calendar
                "fiscal_year": current.year if current.month >= 7 else current.year - 1,
                "fiscal_quarter": ((current.month - 7) % 12) // 3 + 1,
            }
            records.append(record)
            current += timedelta(days=1)
            sk += 1
        
        return records
    
    @staticmethod
    def get_table_definition() -> DimensionTable:
        """Get the date dimension table definition."""
        return DimensionTable(
            name="dim_date",
            columns=[
                Column("date_sk", "int", primary_key=True),
                Column("date_actual", "date", nullable=False),
                Column("day_of_week", "int"),
                Column("day_name", "string"),
                Column("day_of_month", "int"),
                Column("day_of_year", "int"),
                Column("week_of_year", "int"),
                Column("month_number", "int"),
                Column("month_name", "string"),
                Column("month_name_short", "string"),
                Column("quarter", "int"),
                Column("year", "int"),
                Column("year_month", "string"),
                Column("year_quarter", "string"),
                Column("is_weekend", "boolean"),
                Column("is_holiday", "boolean"),
                Column("fiscal_year", "int"),
                Column("fiscal_quarter", "int"),
            ],
            natural_key=["date_actual"],
        )


class TimeDimension:
    """
    Standard time dimension generator.
    Pre-populates time attributes for analysis.
    """
    
    @staticmethod
    def generate(grain_minutes: int = 15) -> List[Dict[str, Any]]:
        """Generate time dimension records."""
        records = []
        sk = 1
        
        for hour in range(24):
            for minute in range(0, 60, grain_minutes):
                time_str = f"{hour:02d}:{minute:02d}:00"
                
                record = {
                    "time_sk": sk,
                    "time_actual": time_str,
                    "hour": hour,
                    "minute": minute,
                    "second": 0,
                    "hour_12": hour % 12 or 12,
                    "am_pm": "AM" if hour < 12 else "PM",
                    "time_of_day": (
                        "Night" if hour < 6 else
                        "Morning" if hour < 12 else
                        "Afternoon" if hour < 18 else
                        "Evening"
                    ),
                    "is_business_hours": 9 <= hour < 17,
                }
                records.append(record)
                sk += 1
        
        return records
    
    @staticmethod
    def get_table_definition() -> DimensionTable:
        """Get the time dimension table definition."""
        return DimensionTable(
            name="dim_time",
            columns=[
                Column("time_sk", "int", primary_key=True),
                Column("time_actual", "string", nullable=False),
                Column("hour", "int"),
                Column("minute", "int"),
                Column("second", "int"),
                Column("hour_12", "int"),
                Column("am_pm", "string"),
                Column("time_of_day", "string"),
                Column("is_business_hours", "boolean"),
            ],
            natural_key=["time_actual"],
        )


# =============================================================================
# Domain-Specific Star Schemas
# =============================================================================

def create_health_star_schema() -> StarSchema:
    """Create star schema for health domain."""
    
    # Fact table: Encounters
    fact_encounter = FactTable(
        name="fact_encounter",
        grain="One row per patient encounter",
        columns=[
            Column("encounter_sk", "int", primary_key=True),
            Column("patient_sk", "int", foreign_key="dim_patient"),
            Column("provider_sk", "int", foreign_key="dim_provider"),
            Column("date_sk", "int", foreign_key="dim_date"),
            Column("time_sk", "int", foreign_key="dim_time"),
            Column("encounter_type_sk", "int", foreign_key="dim_encounter_type"),
            Column("encounter_id", "string"),  # Degenerate dimension
            Column("duration_minutes", "int"),
            Column("total_charges", "float"),
            Column("insurance_paid", "float"),
            Column("patient_paid", "float"),
            Column("diagnosis_count", "int"),
            Column("procedure_count", "int"),
            Column("medication_count", "int"),
        ],
    )
    
    # Dimension: Patient
    dim_patient = DimensionTable(
        name="dim_patient",
        scd_type=SCDType.TYPE_2,
        natural_key=["patient_id"],
        columns=[
            Column("patient_id", "string", nullable=False),
            Column("first_name", "string"),
            Column("last_name", "string"),
            Column("date_of_birth", "date"),
            Column("gender", "string"),
            Column("blood_type", "string"),
            Column("city", "string"),
            Column("state", "string"),
            Column("zip_code", "string"),
            Column("insurance_type", "string"),
            Column("primary_care_provider", "string"),
        ],
    )
    
    # Dimension: Provider
    dim_provider = DimensionTable(
        name="dim_provider",
        natural_key=["provider_id"],
        columns=[
            Column("provider_id", "string", nullable=False),
            Column("first_name", "string"),
            Column("last_name", "string"),
            Column("specialty", "string"),
            Column("department", "string"),
            Column("facility", "string"),
            Column("npi", "string"),
        ],
    )
    
    # Dimension: Encounter Type
    dim_encounter_type = DimensionTable(
        name="dim_encounter_type",
        natural_key=["encounter_type_code"],
        columns=[
            Column("encounter_type_code", "string", nullable=False),
            Column("encounter_type_name", "string"),
            Column("category", "string"),
            Column("is_emergency", "boolean"),
            Column("is_inpatient", "boolean"),
        ],
    )
    
    schema = StarSchema(
        name="health_encounters",
        fact_table=fact_encounter,
    )
    schema.add_dimension(dim_patient)
    schema.add_dimension(dim_provider)
    schema.add_dimension(dim_encounter_type)
    schema.add_dimension(DateDimension.get_table_definition())
    schema.add_dimension(TimeDimension.get_table_definition())
    
    return schema


def create_finance_star_schema() -> StarSchema:
    """Create star schema for finance domain."""
    
    # Fact table: Transactions
    fact_transaction = FactTable(
        name="fact_transaction",
        grain="One row per financial transaction",
        columns=[
            Column("transaction_sk", "int", primary_key=True),
            Column("account_sk", "int", foreign_key="dim_account"),
            Column("customer_sk", "int", foreign_key="dim_customer"),
            Column("merchant_sk", "int", foreign_key="dim_merchant"),
            Column("date_sk", "int", foreign_key="dim_date"),
            Column("time_sk", "int", foreign_key="dim_time"),
            Column("transaction_id", "string"),  # Degenerate dimension
            Column("transaction_type", "string"),
            Column("amount", "float"),
            Column("fee_amount", "float"),
            Column("exchange_rate", "float"),
            Column("fraud_score", "int"),
            Column("is_flagged", "boolean"),
        ],
    )
    
    # Dimension: Account
    dim_account = DimensionTable(
        name="dim_account",
        scd_type=SCDType.TYPE_2,
        natural_key=["account_number"],
        columns=[
            Column("account_number", "string", nullable=False),
            Column("account_type", "string"),
            Column("account_status", "string"),
            Column("currency", "string"),
            Column("opened_date", "date"),
            Column("credit_limit", "float"),
            Column("interest_rate", "float"),
        ],
    )
    
    # Dimension: Customer
    dim_customer = DimensionTable(
        name="dim_customer",
        scd_type=SCDType.TYPE_2,
        natural_key=["customer_id"],
        columns=[
            Column("customer_id", "string", nullable=False),
            Column("customer_name", "string"),
            Column("customer_type", "string"),
            Column("city", "string"),
            Column("state", "string"),
            Column("country", "string"),
            Column("segment", "string"),
            Column("risk_rating", "string"),
        ],
    )
    
    # Dimension: Merchant
    dim_merchant = DimensionTable(
        name="dim_merchant",
        natural_key=["merchant_id"],
        columns=[
            Column("merchant_id", "string"),
            Column("merchant_name", "string"),
            Column("merchant_category", "string"),
            Column("merchant_category_code", "string"),
            Column("city", "string"),
            Column("state", "string"),
            Column("country", "string"),
        ],
    )
    
    schema = StarSchema(
        name="finance_transactions",
        fact_table=fact_transaction,
    )
    schema.add_dimension(dim_account)
    schema.add_dimension(dim_customer)
    schema.add_dimension(dim_merchant)
    schema.add_dimension(DateDimension.get_table_definition())
    schema.add_dimension(TimeDimension.get_table_definition())
    
    return schema


def create_tech_star_schema() -> StarSchema:
    """Create star schema for tech domain."""
    
    # Fact table: Metrics
    fact_metric = FactTable(
        name="fact_server_metric",
        grain="One row per server metric measurement",
        columns=[
            Column("metric_sk", "int", primary_key=True),
            Column("server_sk", "int", foreign_key="dim_server"),
            Column("application_sk", "int", foreign_key="dim_application"),
            Column("date_sk", "int", foreign_key="dim_date"),
            Column("time_sk", "int", foreign_key="dim_time"),
            Column("cpu_usage_pct", "float"),
            Column("memory_used_pct", "float"),
            Column("disk_used_pct", "float"),
            Column("network_in_mb", "float"),
            Column("network_out_mb", "float"),
            Column("request_count", "int"),
            Column("error_count", "int"),
            Column("latency_p50_ms", "float"),
            Column("latency_p99_ms", "float"),
        ],
    )
    
    # Dimension: Server
    dim_server = DimensionTable(
        name="dim_server",
        scd_type=SCDType.TYPE_2,
        natural_key=["server_id"],
        columns=[
            Column("server_id", "string", nullable=False),
            Column("hostname", "string"),
            Column("environment", "string"),
            Column("datacenter", "string"),
            Column("cloud_provider", "string"),
            Column("instance_type", "string"),
            Column("cpu_cores", "int"),
            Column("memory_gb", "int"),
            Column("os_name", "string"),
            Column("os_version", "string"),
            Column("owner_team", "string"),
        ],
    )
    
    # Dimension: Application
    dim_application = DimensionTable(
        name="dim_application",
        natural_key=["app_id"],
        columns=[
            Column("app_id", "string", nullable=False),
            Column("app_name", "string"),
            Column("app_type", "string"),
            Column("framework", "string"),
            Column("language", "string"),
            Column("version", "string"),
            Column("owner_team", "string"),
            Column("criticality", "string"),
        ],
    )
    
    schema = StarSchema(
        name="tech_metrics",
        fact_table=fact_metric,
    )
    schema.add_dimension(dim_server)
    schema.add_dimension(dim_application)
    schema.add_dimension(DateDimension.get_table_definition())
    schema.add_dimension(TimeDimension.get_table_definition())
    
    return schema


def create_university_star_schema() -> StarSchema:
    """Create star schema for university domain."""
    
    # Fact table: Enrollments
    fact_enrollment = FactTable(
        name="fact_enrollment",
        grain="One row per student course enrollment",
        columns=[
            Column("enrollment_sk", "int", primary_key=True),
            Column("student_sk", "int", foreign_key="dim_student"),
            Column("course_sk", "int", foreign_key="dim_course"),
            Column("instructor_sk", "int", foreign_key="dim_instructor"),
            Column("term_sk", "int", foreign_key="dim_term"),
            Column("enrollment_id", "string"),  # Degenerate dimension
            Column("credits_attempted", "int"),
            Column("credits_earned", "int"),
            Column("grade_points", "float"),
            Column("attendance_pct", "float"),
            Column("is_passed", "boolean"),
            Column("is_withdrawn", "boolean"),
        ],
    )
    
    # Dimension: Student
    dim_student = DimensionTable(
        name="dim_student",
        scd_type=SCDType.TYPE_2,
        natural_key=["student_id"],
        columns=[
            Column("student_id", "string", nullable=False),
            Column("first_name", "string"),
            Column("last_name", "string"),
            Column("major", "string"),
            Column("minor", "string"),
            Column("academic_level", "string"),
            Column("enrollment_status", "string"),
            Column("is_international", "boolean"),
            Column("is_transfer", "boolean"),
            Column("admission_year", "int"),
        ],
    )
    
    # Dimension: Course
    dim_course = DimensionTable(
        name="dim_course",
        natural_key=["course_code"],
        columns=[
            Column("course_code", "string", nullable=False),
            Column("course_title", "string"),
            Column("department", "string"),
            Column("credits", "int"),
            Column("level", "string"),
            Column("is_online", "boolean"),
            Column("is_required", "boolean"),
        ],
    )
    
    # Dimension: Instructor
    dim_instructor = DimensionTable(
        name="dim_instructor",
        natural_key=["faculty_id"],
        columns=[
            Column("faculty_id", "string", nullable=False),
            Column("first_name", "string"),
            Column("last_name", "string"),
            Column("department", "string"),
            Column("rank", "string"),
            Column("tenure_status", "string"),
        ],
    )
    
    # Dimension: Term
    dim_term = DimensionTable(
        name="dim_term",
        natural_key=["term_code"],
        columns=[
            Column("term_code", "string", nullable=False),
            Column("term_name", "string"),
            Column("academic_year", "int"),
            Column("semester", "string"),
            Column("start_date", "date"),
            Column("end_date", "date"),
            Column("is_current", "boolean"),
        ],
    )
    
    schema = StarSchema(
        name="university_enrollments",
        fact_table=fact_enrollment,
    )
    schema.add_dimension(dim_student)
    schema.add_dimension(dim_course)
    schema.add_dimension(dim_instructor)
    schema.add_dimension(dim_term)
    schema.add_dimension(DateDimension.get_table_definition())
    
    return schema
