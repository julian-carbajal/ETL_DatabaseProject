"""
dbt-style Macros
================

Reusable transformation functions similar to dbt macros.
"""

from datetime import datetime, date
from typing import Any, Dict, List, Callable
import re


class Macro:
    """A reusable transformation macro."""
    
    def __init__(self, name: str, func: Callable, description: str = ""):
        self.name = name
        self.func = func
        self.description = description
    
    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


class MacroLibrary:
    """Library of common transformation macros."""
    
    @staticmethod
    def generate_surrogate_key(*columns) -> Callable:
        """Generate a surrogate key from multiple columns."""
        import hashlib
        
        def _generate(record: Dict[str, Any]) -> str:
            values = [str(record.get(col, "")) for col in columns]
            combined = "|".join(values)
            return hashlib.md5(combined.encode()).hexdigest()[:16]
        
        return _generate
    
    @staticmethod
    def safe_cast(value: Any, target_type: str, default: Any = None) -> Any:
        """Safely cast a value to target type."""
        if value is None:
            return default
        
        try:
            if target_type == "int":
                return int(float(value))
            elif target_type == "float":
                return float(value)
            elif target_type == "string":
                return str(value)
            elif target_type == "boolean":
                if isinstance(value, bool):
                    return value
                return str(value).lower() in ("true", "1", "yes")
            elif target_type == "date":
                if isinstance(value, date):
                    return value
                return datetime.fromisoformat(str(value)).date()
            elif target_type == "datetime":
                if isinstance(value, datetime):
                    return value
                return datetime.fromisoformat(str(value))
        except (ValueError, TypeError):
            return default
        
        return value
    
    @staticmethod
    def coalesce(*values) -> Any:
        """Return first non-null value."""
        for v in values:
            if v is not None:
                return v
        return None
    
    @staticmethod
    def nullif(value: Any, null_value: Any) -> Any:
        """Return null if value equals null_value."""
        return None if value == null_value else value
    
    @staticmethod
    def date_trunc(date_value: Any, granularity: str) -> date:
        """Truncate date to specified granularity."""
        if isinstance(date_value, str):
            date_value = datetime.fromisoformat(date_value)
        if isinstance(date_value, datetime):
            date_value = date_value.date()
        
        if granularity == "year":
            return date(date_value.year, 1, 1)
        elif granularity == "quarter":
            quarter_month = ((date_value.month - 1) // 3) * 3 + 1
            return date(date_value.year, quarter_month, 1)
        elif granularity == "month":
            return date(date_value.year, date_value.month, 1)
        elif granularity == "week":
            days_since_monday = date_value.weekday()
            return date_value - timedelta(days=days_since_monday)
        
        return date_value
    
    @staticmethod
    def date_diff(date1: Any, date2: Any, unit: str = "days") -> int:
        """Calculate difference between two dates."""
        if isinstance(date1, str):
            date1 = datetime.fromisoformat(date1)
        if isinstance(date2, str):
            date2 = datetime.fromisoformat(date2)
        
        if isinstance(date1, datetime):
            date1 = date1.date()
        if isinstance(date2, datetime):
            date2 = date2.date()
        
        diff = (date2 - date1).days
        
        if unit == "days":
            return diff
        elif unit == "weeks":
            return diff // 7
        elif unit == "months":
            return diff // 30
        elif unit == "years":
            return diff // 365
        
        return diff
    
    @staticmethod
    def clean_string(value: str) -> str:
        """Clean and normalize a string."""
        if not value:
            return ""
        
        # Trim whitespace
        value = value.strip()
        
        # Normalize whitespace
        value = re.sub(r'\s+', ' ', value)
        
        return value
    
    @staticmethod
    def extract_email_domain(email: str) -> str:
        """Extract domain from email address."""
        if not email or "@" not in email:
            return ""
        return email.split("@")[-1].lower()
    
    @staticmethod
    def mask_pii(value: str, mask_char: str = "*", visible_chars: int = 4) -> str:
        """Mask PII data, keeping last N characters visible."""
        if not value:
            return ""
        
        if len(value) <= visible_chars:
            return mask_char * len(value)
        
        masked_length = len(value) - visible_chars
        return mask_char * masked_length + value[-visible_chars:]
    
    @staticmethod
    def categorize(value: Any, buckets: List[tuple]) -> str:
        """
        Categorize a value into buckets.
        
        buckets: List of (threshold, label) tuples, sorted ascending
        Example: [(0, "Low"), (50, "Medium"), (100, "High")]
        """
        if value is None:
            return "Unknown"
        
        result = buckets[0][1] if buckets else "Unknown"
        
        for threshold, label in buckets:
            if value >= threshold:
                result = label
        
        return result
    
    @staticmethod
    def pivot_rows(
        records: List[Dict],
        group_by: str,
        pivot_column: str,
        value_column: str,
        agg_func: str = "sum",
    ) -> List[Dict]:
        """Pivot rows to columns."""
        from collections import defaultdict
        
        # Group data
        groups = defaultdict(lambda: defaultdict(list))
        pivot_values = set()
        
        for record in records:
            group_key = record.get(group_by)
            pivot_val = record.get(pivot_column)
            value = record.get(value_column)
            
            groups[group_key][pivot_val].append(value)
            pivot_values.add(pivot_val)
        
        # Aggregate and pivot
        result = []
        for group_key, pivot_data in groups.items():
            row = {group_by: group_key}
            
            for pv in pivot_values:
                values = pivot_data.get(pv, [])
                if agg_func == "sum":
                    row[f"{pivot_column}_{pv}"] = sum(v for v in values if v is not None)
                elif agg_func == "count":
                    row[f"{pivot_column}_{pv}"] = len(values)
                elif agg_func == "avg":
                    row[f"{pivot_column}_{pv}"] = (
                        sum(v for v in values if v is not None) / len(values)
                        if values else 0
                    )
            
            result.append(row)
        
        return result
    
    @staticmethod
    def unpivot_columns(
        records: List[Dict],
        id_columns: List[str],
        value_columns: List[str],
        var_name: str = "variable",
        value_name: str = "value",
    ) -> List[Dict]:
        """Unpivot columns to rows."""
        result = []
        
        for record in records:
            base = {col: record.get(col) for col in id_columns}
            
            for col in value_columns:
                row = {
                    **base,
                    var_name: col,
                    value_name: record.get(col),
                }
                result.append(row)
        
        return result


# Import timedelta for date_trunc
from datetime import timedelta
