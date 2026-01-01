"""
Data Validation Framework.
Provides schema validation, business rules, and data quality checks.
"""

import re
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Severity levels for validation failures."""
    ERROR = "ERROR"      # Critical - must be fixed
    WARNING = "WARNING"  # Should be reviewed
    INFO = "INFO"        # Informational only


@dataclass
class ValidationResult:
    """Result of a validation check."""
    is_valid: bool
    rule_name: str
    field_name: Optional[str] = None
    message: str = ""
    severity: ValidationSeverity = ValidationSeverity.ERROR
    failed_value: Any = None
    record_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "rule_name": self.rule_name,
            "field_name": self.field_name,
            "message": self.message,
            "severity": self.severity.value,
            "failed_value": str(self.failed_value)[:100] if self.failed_value else None,
            "record_id": self.record_id,
        }


@dataclass
class ValidationReport:
    """Aggregated validation report."""
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    error_count: int = 0
    warning_count: int = 0
    results: List[ValidationResult] = field(default_factory=list)
    
    @property
    def validity_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return self.valid_records / self.total_records
    
    def add_result(self, result: ValidationResult):
        self.results.append(result)
        if not result.is_valid:
            if result.severity == ValidationSeverity.ERROR:
                self.error_count += 1
            elif result.severity == ValidationSeverity.WARNING:
                self.warning_count += 1
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "validity_rate": round(self.validity_rate, 4),
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "sample_failures": [r.to_dict() for r in self.results[:20]],
        }


class DataValidator:
    """
    Main data validator class.
    Validates records against defined rules.
    """
    
    def __init__(self, name: str = "DataValidator"):
        self.name = name
        self.rules: List[Dict[str, Any]] = []
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    def add_rule(
        self,
        field: str,
        rule_type: str,
        params: Dict[str, Any] = None,
        severity: ValidationSeverity = ValidationSeverity.ERROR,
        message: str = None,
    ) -> "DataValidator":
        """Add a validation rule."""
        self.rules.append({
            "field": field,
            "rule_type": rule_type,
            "params": params or {},
            "severity": severity,
            "message": message,
        })
        return self
    
    def required(self, field: str, severity: ValidationSeverity = ValidationSeverity.ERROR) -> "DataValidator":
        """Add required field rule."""
        return self.add_rule(field, "required", severity=severity)
    
    def type_check(self, field: str, expected_type: str, severity: ValidationSeverity = ValidationSeverity.ERROR) -> "DataValidator":
        """Add type check rule."""
        return self.add_rule(field, "type", {"expected_type": expected_type}, severity=severity)
    
    def range_check(self, field: str, min_val: float = None, max_val: float = None, severity: ValidationSeverity = ValidationSeverity.ERROR) -> "DataValidator":
        """Add range check rule."""
        return self.add_rule(field, "range", {"min": min_val, "max": max_val}, severity=severity)
    
    def pattern(self, field: str, regex: str, severity: ValidationSeverity = ValidationSeverity.ERROR) -> "DataValidator":
        """Add regex pattern rule."""
        return self.add_rule(field, "pattern", {"regex": regex}, severity=severity)
    
    def enum_check(self, field: str, allowed_values: List[Any], severity: ValidationSeverity = ValidationSeverity.ERROR) -> "DataValidator":
        """Add enum/allowed values rule."""
        return self.add_rule(field, "enum", {"allowed": allowed_values}, severity=severity)
    
    def unique(self, field: str, severity: ValidationSeverity = ValidationSeverity.ERROR) -> "DataValidator":
        """Add uniqueness rule (checked across all records)."""
        return self.add_rule(field, "unique", severity=severity)
    
    def custom(self, field: str, validator: Callable[[Any], bool], message: str, severity: ValidationSeverity = ValidationSeverity.ERROR) -> "DataValidator":
        """Add custom validation function."""
        return self.add_rule(field, "custom", {"validator": validator, "message": message}, severity=severity)
    
    def validate_record(self, record: Dict[str, Any], record_id: str = None) -> List[ValidationResult]:
        """Validate a single record against all rules."""
        results = []
        
        for rule in self.rules:
            field = rule["field"]
            rule_type = rule["rule_type"]
            params = rule["params"]
            severity = rule["severity"]
            
            value = record.get(field)
            result = self._apply_rule(field, value, rule_type, params, severity, record_id)
            
            if result:
                results.append(result)
        
        return results
    
    def _apply_rule(
        self,
        field: str,
        value: Any,
        rule_type: str,
        params: Dict[str, Any],
        severity: ValidationSeverity,
        record_id: str = None,
    ) -> Optional[ValidationResult]:
        """Apply a single validation rule."""
        
        if rule_type == "required":
            if value is None or (isinstance(value, str) and value.strip() == ""):
                return ValidationResult(
                    is_valid=False,
                    rule_name="required",
                    field_name=field,
                    message=f"Field '{field}' is required but missing or empty",
                    severity=severity,
                    failed_value=value,
                    record_id=record_id,
                )
        
        elif rule_type == "type":
            if value is not None:
                expected = params["expected_type"]
                type_map = {
                    "string": str,
                    "int": int,
                    "float": (int, float),
                    "bool": bool,
                    "list": list,
                    "dict": dict,
                }
                expected_types = type_map.get(expected, str)
                if not isinstance(value, expected_types):
                    return ValidationResult(
                        is_valid=False,
                        rule_name="type_check",
                        field_name=field,
                        message=f"Field '{field}' expected {expected}, got {type(value).__name__}",
                        severity=severity,
                        failed_value=value,
                        record_id=record_id,
                    )
        
        elif rule_type == "range":
            if value is not None:
                try:
                    num_value = float(value)
                    min_val = params.get("min")
                    max_val = params.get("max")
                    
                    if min_val is not None and num_value < min_val:
                        return ValidationResult(
                            is_valid=False,
                            rule_name="range_check",
                            field_name=field,
                            message=f"Field '{field}' value {value} is below minimum {min_val}",
                            severity=severity,
                            failed_value=value,
                            record_id=record_id,
                        )
                    if max_val is not None and num_value > max_val:
                        return ValidationResult(
                            is_valid=False,
                            rule_name="range_check",
                            field_name=field,
                            message=f"Field '{field}' value {value} exceeds maximum {max_val}",
                            severity=severity,
                            failed_value=value,
                            record_id=record_id,
                        )
                except (ValueError, TypeError):
                    return ValidationResult(
                        is_valid=False,
                        rule_name="range_check",
                        field_name=field,
                        message=f"Field '{field}' value '{value}' is not numeric",
                        severity=severity,
                        failed_value=value,
                        record_id=record_id,
                    )
        
        elif rule_type == "pattern":
            if value is not None and isinstance(value, str):
                regex = params["regex"]
                if not re.match(regex, value):
                    return ValidationResult(
                        is_valid=False,
                        rule_name="pattern",
                        field_name=field,
                        message=f"Field '{field}' value '{value}' does not match pattern",
                        severity=severity,
                        failed_value=value,
                        record_id=record_id,
                    )
        
        elif rule_type == "enum":
            if value is not None:
                allowed = params["allowed"]
                if value not in allowed:
                    return ValidationResult(
                        is_valid=False,
                        rule_name="enum_check",
                        field_name=field,
                        message=f"Field '{field}' value '{value}' not in allowed values",
                        severity=severity,
                        failed_value=value,
                        record_id=record_id,
                    )
        
        elif rule_type == "custom":
            if value is not None:
                validator = params["validator"]
                custom_message = params.get("message", "Custom validation failed")
                try:
                    if not validator(value):
                        return ValidationResult(
                            is_valid=False,
                            rule_name="custom",
                            field_name=field,
                            message=f"Field '{field}': {custom_message}",
                            severity=severity,
                            failed_value=value,
                            record_id=record_id,
                        )
                except Exception as e:
                    return ValidationResult(
                        is_valid=False,
                        rule_name="custom",
                        field_name=field,
                        message=f"Field '{field}': validation error - {e}",
                        severity=severity,
                        failed_value=value,
                        record_id=record_id,
                    )
        
        return None  # Validation passed
    
    def validate(
        self,
        data: List[Dict[str, Any]],
        id_field: str = None,
    ) -> ValidationReport:
        """Validate all records and return report."""
        report = ValidationReport(total_records=len(data))
        seen_values: Dict[str, set] = {}  # For uniqueness checks
        
        # Initialize seen_values for unique rules
        for rule in self.rules:
            if rule["rule_type"] == "unique":
                seen_values[rule["field"]] = set()
        
        for i, record in enumerate(data):
            record_id = str(record.get(id_field, i)) if id_field else str(i)
            record_valid = True
            
            # Standard validations
            results = self.validate_record(record, record_id)
            for result in results:
                report.add_result(result)
                if not result.is_valid and result.severity == ValidationSeverity.ERROR:
                    record_valid = False
            
            # Uniqueness checks
            for rule in self.rules:
                if rule["rule_type"] == "unique":
                    field = rule["field"]
                    value = record.get(field)
                    if value is not None:
                        if value in seen_values[field]:
                            result = ValidationResult(
                                is_valid=False,
                                rule_name="unique",
                                field_name=field,
                                message=f"Field '{field}' value '{value}' is not unique",
                                severity=rule["severity"],
                                failed_value=value,
                                record_id=record_id,
                            )
                            report.add_result(result)
                            if rule["severity"] == ValidationSeverity.ERROR:
                                record_valid = False
                        else:
                            seen_values[field].add(value)
            
            if record_valid:
                report.valid_records += 1
            else:
                report.invalid_records += 1
        
        return report


class SchemaValidator:
    """Validate data against a schema definition."""
    
    def __init__(self, schema: Dict[str, Dict[str, Any]]):
        """
        Initialize with schema definition.
        
        Schema format:
        {
            "field_name": {
                "type": "string|int|float|bool|date|datetime",
                "required": True|False,
                "nullable": True|False,
                "min": value,
                "max": value,
                "pattern": "regex",
                "enum": [values],
            }
        }
        """
        self.schema = schema
        self.validator = DataValidator("SchemaValidator")
        self._build_validator()
    
    def _build_validator(self):
        """Build validator from schema."""
        for field, rules in self.schema.items():
            if rules.get("required", False):
                self.validator.required(field)
            
            if "type" in rules:
                self.validator.type_check(field, rules["type"])
            
            if "min" in rules or "max" in rules:
                self.validator.range_check(
                    field,
                    min_val=rules.get("min"),
                    max_val=rules.get("max")
                )
            
            if "pattern" in rules:
                self.validator.pattern(field, rules["pattern"])
            
            if "enum" in rules:
                self.validator.enum_check(field, rules["enum"])
    
    def validate(self, data: List[Dict[str, Any]], id_field: str = None) -> ValidationReport:
        """Validate data against schema."""
        return self.validator.validate(data, id_field)


class BusinessRuleValidator:
    """Validate data against business rules."""
    
    def __init__(self, name: str = "BusinessRuleValidator"):
        self.name = name
        self.rules: List[Dict[str, Any]] = []
    
    def add_rule(
        self,
        name: str,
        condition: Callable[[Dict[str, Any]], bool],
        message: str,
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ) -> "BusinessRuleValidator":
        """Add a business rule."""
        self.rules.append({
            "name": name,
            "condition": condition,
            "message": message,
            "severity": severity,
        })
        return self
    
    def validate(
        self,
        data: List[Dict[str, Any]],
        id_field: str = None,
    ) -> ValidationReport:
        """Validate all records against business rules."""
        report = ValidationReport(total_records=len(data))
        
        for i, record in enumerate(data):
            record_id = str(record.get(id_field, i)) if id_field else str(i)
            record_valid = True
            
            for rule in self.rules:
                try:
                    if not rule["condition"](record):
                        result = ValidationResult(
                            is_valid=False,
                            rule_name=rule["name"],
                            message=rule["message"],
                            severity=rule["severity"],
                            record_id=record_id,
                        )
                        report.add_result(result)
                        if rule["severity"] == ValidationSeverity.ERROR:
                            record_valid = False
                except Exception as e:
                    result = ValidationResult(
                        is_valid=False,
                        rule_name=rule["name"],
                        message=f"Rule evaluation error: {e}",
                        severity=ValidationSeverity.ERROR,
                        record_id=record_id,
                    )
                    report.add_result(result)
                    record_valid = False
            
            if record_valid:
                report.valid_records += 1
            else:
                report.invalid_records += 1
        
        return report


# Pre-built validators for common domains
def create_health_validator() -> DataValidator:
    """Create validator for health domain data."""
    return (DataValidator("HealthValidator")
        .required("patient_id")
        .pattern("patient_id", r"^PAT\d{8}$")
        .required("first_name")
        .required("last_name")
        .required("date_of_birth")
        .enum_check("gender", ["M", "F", "O", "U"], ValidationSeverity.WARNING)
        .pattern("email", r"^[\w\.-]+@[\w\.-]+\.\w+$", ValidationSeverity.WARNING)
        .range_check("blood_pressure_systolic", 60, 250, ValidationSeverity.WARNING)
        .range_check("blood_pressure_diastolic", 40, 150, ValidationSeverity.WARNING)
        .range_check("heart_rate", 30, 220, ValidationSeverity.WARNING)
    )


def create_finance_validator() -> DataValidator:
    """Create validator for finance domain data."""
    return (DataValidator("FinanceValidator")
        .required("account_number")
        .pattern("account_number", r"^ACC\d{10}$")
        .required("customer_id")
        .required("account_type")
        .enum_check("account_type", ["CHECKING", "SAVINGS", "MONEY_MARKET", "CD", "BROKERAGE", "CREDIT_CARD"])
        .enum_check("currency", ["USD", "EUR", "GBP", "JPY", "CNY"])
        .enum_check("status", ["ACTIVE", "FROZEN", "CLOSED", "DORMANT"])
        .range_check("aml_risk_score", 0, 100, ValidationSeverity.WARNING)
    )


def create_tech_validator() -> DataValidator:
    """Create validator for tech domain data."""
    return (DataValidator("TechValidator")
        .required("server_id")
        .pattern("server_id", r"^SRV-[A-Z]{2}-\d{4}$")
        .required("hostname")
        .required("environment")
        .enum_check("environment", ["PROD", "STAGING", "DEV", "QA"])
        .enum_check("status", ["RUNNING", "STOPPED", "MAINTENANCE", "DEGRADED", "FAILED"])
        .range_check("cpu_usage_pct", 0, 100, ValidationSeverity.WARNING)
        .range_check("memory_used_pct", 0, 100, ValidationSeverity.WARNING)
    )


def create_university_validator() -> DataValidator:
    """Create validator for university domain data."""
    return (DataValidator("UniversityValidator")
        .required("student_id")
        .pattern("student_id", r"^STU\d{8}$")
        .required("first_name")
        .required("last_name")
        .required("email")
        .pattern("email", r"^[\w\.-]+@[\w\.-]+\.\w+$")
        .range_check("gpa", 0.0, 4.0)
        .range_check("credits_earned", 0, 200)
        .enum_check("enrollment_status", ["ACTIVE", "GRADUATED", "SUSPENDED", "WITHDRAWN", "ON_LEAVE"])
        .enum_check("academic_level", ["FRESHMAN", "SOPHOMORE", "JUNIOR", "SENIOR", "GRADUATE"])
    )
