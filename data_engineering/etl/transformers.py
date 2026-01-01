"""
Data Transformers for ETL Pipeline.
Provides cleansing, enrichment, and aggregation transformations.
"""

import re
import hashlib
import time
from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Callable, Union
from decimal import Decimal, InvalidOperation
import logging

from .base import ETLStep, ETLContext

logger = logging.getLogger(__name__)


class DataTransformer(ETLStep, ABC):
    """Abstract base class for data transformers."""
    
    def __init__(self, name: str, description: str = ""):
        super().__init__(name, description)
    
    @abstractmethod
    def transform(self, data: List[Dict[str, Any]], context: ETLContext) -> List[Dict[str, Any]]:
        """Transform data."""
        pass
    
    def execute(self, context: ETLContext) -> ETLContext:
        """Execute transformation step."""
        start_time = time.time()
        
        if context.raw_data is None:
            raise ValueError("No data to transform")
        
        try:
            transformed = self.transform(context.raw_data, context)
            context.transformed_data = transformed
            context.metrics.records_transformed = len(transformed)
            context.metrics.transformation_time_seconds = time.time() - start_time
            
            self.logger.info(
                f"Transformed {context.metrics.records_transformed} records "
                f"in {context.metrics.transformation_time_seconds:.2f}s"
            )
            
        except Exception as e:
            context.add_error(e, "transformation")
            raise
        
        return context


class CleansingTransformer(DataTransformer):
    """Clean and standardize data."""
    
    def __init__(
        self,
        name: str = "CleansingTransformer",
        trim_strings: bool = True,
        remove_nulls: bool = False,
        null_values: List[str] = None,
        lowercase_columns: List[str] = None,
        uppercase_columns: List[str] = None,
        date_columns: Dict[str, str] = None,
        numeric_columns: List[str] = None,
    ):
        super().__init__(name, "Clean and standardize data")
        self.trim_strings = trim_strings
        self.remove_nulls = remove_nulls
        self.null_values = null_values or ["", "NULL", "null", "None", "N/A", "n/a", "NA"]
        self.lowercase_columns = lowercase_columns or []
        self.uppercase_columns = uppercase_columns or []
        self.date_columns = date_columns or {}  # {column: format}
        self.numeric_columns = numeric_columns or []
    
    def transform(self, data: List[Dict[str, Any]], context: ETLContext) -> List[Dict[str, Any]]:
        """Apply cleansing transformations."""
        cleaned = []
        rejected = 0
        
        for record in data:
            try:
                clean_record = self._clean_record(record)
                
                if self.remove_nulls and self._has_critical_nulls(clean_record):
                    rejected += 1
                    continue
                
                cleaned.append(clean_record)
                
            except Exception as e:
                context.add_warning(f"Record cleansing failed: {e}")
                rejected += 1
        
        context.metrics.records_rejected += rejected
        return cleaned
    
    def _clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean a single record."""
        cleaned = {}
        
        for key, value in record.items():
            # Trim strings
            if isinstance(value, str) and self.trim_strings:
                value = value.strip()
            
            # Handle null values
            if value in self.null_values:
                value = None
            
            # Case transformations
            if key in self.lowercase_columns and isinstance(value, str):
                value = value.lower()
            elif key in self.uppercase_columns and isinstance(value, str):
                value = value.upper()
            
            # Date parsing
            if key in self.date_columns and value:
                try:
                    value = datetime.strptime(str(value), self.date_columns[key])
                except ValueError:
                    pass
            
            # Numeric conversion
            if key in self.numeric_columns and value is not None:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    pass
            
            cleaned[key] = value
        
        return cleaned
    
    def _has_critical_nulls(self, record: Dict[str, Any]) -> bool:
        """Check if record has critical null values."""
        # Override in subclass to define critical fields
        return False


class EnrichmentTransformer(DataTransformer):
    """Enrich data with additional fields and lookups."""
    
    def __init__(
        self,
        name: str = "EnrichmentTransformer",
        computed_fields: Dict[str, Callable[[Dict], Any]] = None,
        lookup_tables: Dict[str, Dict[str, Any]] = None,
        default_values: Dict[str, Any] = None,
    ):
        super().__init__(name, "Enrich data with computed and lookup fields")
        self.computed_fields = computed_fields or {}
        self.lookup_tables = lookup_tables or {}
        self.default_values = default_values or {}
    
    def transform(self, data: List[Dict[str, Any]], context: ETLContext) -> List[Dict[str, Any]]:
        """Apply enrichment transformations."""
        enriched = []
        
        for record in data:
            enriched_record = record.copy()
            
            # Add default values
            for field, default in self.default_values.items():
                if field not in enriched_record or enriched_record[field] is None:
                    enriched_record[field] = default
            
            # Add computed fields
            for field, compute_fn in self.computed_fields.items():
                try:
                    enriched_record[field] = compute_fn(enriched_record)
                except Exception as e:
                    context.add_warning(f"Computed field {field} failed: {e}")
                    enriched_record[field] = None
            
            # Apply lookups
            for lookup_field, lookup_table in self.lookup_tables.items():
                if lookup_field in enriched_record:
                    key = enriched_record[lookup_field]
                    if key in lookup_table:
                        enriched_record[f"{lookup_field}_lookup"] = lookup_table[key]
            
            # Add metadata
            enriched_record["_etl_timestamp"] = datetime.utcnow().isoformat()
            enriched_record["_etl_job_id"] = context.job_id
            
            enriched.append(enriched_record)
        
        return enriched


class ValidationTransformer(DataTransformer):
    """Validate data against rules and schemas."""
    
    def __init__(
        self,
        name: str = "ValidationTransformer",
        rules: Dict[str, Dict[str, Any]] = None,
        reject_invalid: bool = False,
    ):
        super().__init__(name, "Validate data against rules")
        self.rules = rules or {}
        self.reject_invalid = reject_invalid
    
    def transform(self, data: List[Dict[str, Any]], context: ETLContext) -> List[Dict[str, Any]]:
        """Apply validation rules."""
        validated = []
        
        for record in data:
            is_valid, errors = self._validate_record(record)
            
            if not is_valid:
                if self.reject_invalid:
                    context.metrics.records_rejected += 1
                    for error in errors:
                        context.add_warning(f"Validation failed: {error}")
                    continue
                else:
                    record["_validation_errors"] = errors
            
            record["_is_valid"] = is_valid
            validated.append(record)
        
        return validated
    
    def _validate_record(self, record: Dict[str, Any]) -> tuple:
        """Validate a single record against rules."""
        errors = []
        
        for field, rule in self.rules.items():
            value = record.get(field)
            
            # Required check
            if rule.get("required") and value is None:
                errors.append(f"{field} is required")
                continue
            
            if value is None:
                continue
            
            # Type check
            expected_type = rule.get("type")
            if expected_type:
                if expected_type == "string" and not isinstance(value, str):
                    errors.append(f"{field} must be a string")
                elif expected_type == "int" and not isinstance(value, int):
                    errors.append(f"{field} must be an integer")
                elif expected_type == "float" and not isinstance(value, (int, float)):
                    errors.append(f"{field} must be a number")
            
            # Range check
            if "min" in rule and value < rule["min"]:
                errors.append(f"{field} must be >= {rule['min']}")
            if "max" in rule and value > rule["max"]:
                errors.append(f"{field} must be <= {rule['max']}")
            
            # Pattern check
            if "pattern" in rule and isinstance(value, str):
                if not re.match(rule["pattern"], value):
                    errors.append(f"{field} does not match pattern {rule['pattern']}")
            
            # Enum check
            if "enum" in rule and value not in rule["enum"]:
                errors.append(f"{field} must be one of {rule['enum']}")
        
        return len(errors) == 0, errors


class DeduplicationTransformer(DataTransformer):
    """Remove duplicate records."""
    
    def __init__(
        self,
        name: str = "DeduplicationTransformer",
        key_columns: List[str] = None,
        keep: str = "first",  # first, last, none
    ):
        super().__init__(name, "Remove duplicate records")
        self.key_columns = key_columns or []
        self.keep = keep
    
    def transform(self, data: List[Dict[str, Any]], context: ETLContext) -> List[Dict[str, Any]]:
        """Remove duplicates based on key columns."""
        if not self.key_columns:
            return data
        
        seen = {}
        deduplicated = []
        duplicates = 0
        
        for record in data:
            key = tuple(record.get(col) for col in self.key_columns)
            
            if key in seen:
                duplicates += 1
                if self.keep == "last":
                    # Replace previous record
                    idx = seen[key]
                    deduplicated[idx] = record
                # If keep == "first" or "none", skip
            else:
                seen[key] = len(deduplicated)
                deduplicated.append(record)
        
        context.metrics.records_duplicates = duplicates
        self.logger.info(f"Removed {duplicates} duplicate records")
        
        return deduplicated


class AggregationTransformer(DataTransformer):
    """Aggregate data by groups."""
    
    def __init__(
        self,
        name: str = "AggregationTransformer",
        group_by: List[str] = None,
        aggregations: Dict[str, Dict[str, str]] = None,
    ):
        """
        Args:
            group_by: Columns to group by
            aggregations: {output_column: {"column": source_col, "function": "sum|avg|count|min|max"}}
        """
        super().__init__(name, "Aggregate data by groups")
        self.group_by = group_by or []
        self.aggregations = aggregations or {}
    
    def transform(self, data: List[Dict[str, Any]], context: ETLContext) -> List[Dict[str, Any]]:
        """Aggregate data."""
        if not self.group_by:
            return data
        
        groups = {}
        
        for record in data:
            key = tuple(record.get(col) for col in self.group_by)
            
            if key not in groups:
                groups[key] = []
            groups[key].append(record)
        
        aggregated = []
        
        for key, records in groups.items():
            agg_record = dict(zip(self.group_by, key))
            
            for output_col, agg_config in self.aggregations.items():
                source_col = agg_config["column"]
                func = agg_config["function"]
                
                values = [
                    r.get(source_col) for r in records
                    if r.get(source_col) is not None
                ]
                
                if func == "sum":
                    agg_record[output_col] = sum(values) if values else 0
                elif func == "avg":
                    agg_record[output_col] = sum(values) / len(values) if values else 0
                elif func == "count":
                    agg_record[output_col] = len(values)
                elif func == "min":
                    agg_record[output_col] = min(values) if values else None
                elif func == "max":
                    agg_record[output_col] = max(values) if values else None
            
            aggregated.append(agg_record)
        
        return aggregated


class PIIMaskingTransformer(DataTransformer):
    """Mask PII (Personally Identifiable Information) data."""
    
    def __init__(
        self,
        name: str = "PIIMaskingTransformer",
        hash_columns: List[str] = None,
        mask_columns: Dict[str, str] = None,  # {column: mask_pattern}
        redact_columns: List[str] = None,
    ):
        super().__init__(name, "Mask PII data for compliance")
        self.hash_columns = hash_columns or []
        self.mask_columns = mask_columns or {}
        self.redact_columns = redact_columns or []
    
    def transform(self, data: List[Dict[str, Any]], context: ETLContext) -> List[Dict[str, Any]]:
        """Apply PII masking."""
        masked = []
        
        for record in data:
            masked_record = record.copy()
            
            # Hash columns (one-way, for matching)
            for col in self.hash_columns:
                if col in masked_record and masked_record[col]:
                    value = str(masked_record[col])
                    masked_record[f"{col}_hash"] = hashlib.sha256(
                        value.encode()
                    ).hexdigest()
                    masked_record[col] = None
            
            # Mask columns (partial visibility)
            for col, pattern in self.mask_columns.items():
                if col in masked_record and masked_record[col]:
                    value = str(masked_record[col])
                    if pattern == "email":
                        parts = value.split("@")
                        if len(parts) == 2:
                            masked_record[col] = f"{parts[0][:2]}***@{parts[1]}"
                    elif pattern == "phone":
                        masked_record[col] = f"***-***-{value[-4:]}"
                    elif pattern == "ssn":
                        masked_record[col] = f"***-**-{value[-4:]}"
                    elif pattern == "credit_card":
                        masked_record[col] = f"****-****-****-{value[-4:]}"
            
            # Redact columns (complete removal)
            for col in self.redact_columns:
                if col in masked_record:
                    masked_record[col] = "[REDACTED]"
            
            masked.append(masked_record)
        
        return masked


class TypeCastTransformer(DataTransformer):
    """Cast data types for database compatibility."""
    
    def __init__(
        self,
        name: str = "TypeCastTransformer",
        type_mappings: Dict[str, str] = None,
    ):
        """
        Args:
            type_mappings: {column: "int|float|decimal|string|date|datetime|bool"}
        """
        super().__init__(name, "Cast data types")
        self.type_mappings = type_mappings or {}
    
    def transform(self, data: List[Dict[str, Any]], context: ETLContext) -> List[Dict[str, Any]]:
        """Apply type casting."""
        casted = []
        
        for record in data:
            casted_record = record.copy()
            
            for col, target_type in self.type_mappings.items():
                if col not in casted_record:
                    continue
                
                value = casted_record[col]
                if value is None:
                    continue
                
                try:
                    if target_type == "int":
                        casted_record[col] = int(float(value))
                    elif target_type == "float":
                        casted_record[col] = float(value)
                    elif target_type == "decimal":
                        casted_record[col] = Decimal(str(value))
                    elif target_type == "string":
                        casted_record[col] = str(value)
                    elif target_type == "bool":
                        casted_record[col] = bool(value) if not isinstance(value, str) else value.lower() in ("true", "1", "yes")
                    elif target_type == "date":
                        if isinstance(value, str):
                            casted_record[col] = datetime.fromisoformat(value).date()
                    elif target_type == "datetime":
                        if isinstance(value, str):
                            casted_record[col] = datetime.fromisoformat(value)
                except (ValueError, InvalidOperation) as e:
                    context.add_warning(f"Type cast failed for {col}: {e}")
            
            casted.append(casted_record)
        
        return casted
