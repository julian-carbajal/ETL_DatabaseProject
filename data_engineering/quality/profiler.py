"""
Data Profiler for analyzing data quality and statistics.
Provides comprehensive data profiling capabilities.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from collections import Counter
import statistics
import logging

logger = logging.getLogger(__name__)


@dataclass
class ColumnProfile:
    """Profile statistics for a single column."""
    column_name: str
    data_type: str = "unknown"
    total_count: int = 0
    null_count: int = 0
    unique_count: int = 0
    
    # Numeric statistics
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    std_dev: Optional[float] = None
    sum_value: Optional[float] = None
    
    # String statistics
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    
    # Distribution
    top_values: List[tuple] = field(default_factory=list)
    
    @property
    def null_rate(self) -> float:
        if self.total_count == 0:
            return 0.0
        return self.null_count / self.total_count
    
    @property
    def uniqueness_rate(self) -> float:
        non_null = self.total_count - self.null_count
        if non_null == 0:
            return 0.0
        return self.unique_count / non_null
    
    @property
    def completeness(self) -> float:
        return 1.0 - self.null_rate
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "column_name": self.column_name,
            "data_type": self.data_type,
            "total_count": self.total_count,
            "null_count": self.null_count,
            "null_rate": round(self.null_rate, 4),
            "unique_count": self.unique_count,
            "uniqueness_rate": round(self.uniqueness_rate, 4),
            "completeness": round(self.completeness, 4),
            "min_value": self.min_value,
            "max_value": self.max_value,
            "mean_value": round(self.mean_value, 4) if self.mean_value else None,
            "median_value": self.median_value,
            "std_dev": round(self.std_dev, 4) if self.std_dev else None,
            "min_length": self.min_length,
            "max_length": self.max_length,
            "avg_length": round(self.avg_length, 2) if self.avg_length else None,
            "top_values": self.top_values[:10],
        }


@dataclass
class DataProfile:
    """Complete profile for a dataset."""
    name: str
    profiled_at: datetime = field(default_factory=datetime.utcnow)
    row_count: int = 0
    column_count: int = 0
    columns: Dict[str, ColumnProfile] = field(default_factory=dict)
    
    # Dataset-level metrics
    duplicate_row_count: int = 0
    memory_size_bytes: int = 0
    
    @property
    def duplicate_rate(self) -> float:
        if self.row_count == 0:
            return 0.0
        return self.duplicate_row_count / self.row_count
    
    @property
    def overall_completeness(self) -> float:
        if not self.columns:
            return 0.0
        return sum(c.completeness for c in self.columns.values()) / len(self.columns)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "profiled_at": self.profiled_at.isoformat(),
            "row_count": self.row_count,
            "column_count": self.column_count,
            "duplicate_row_count": self.duplicate_row_count,
            "duplicate_rate": round(self.duplicate_rate, 4),
            "overall_completeness": round(self.overall_completeness, 4),
            "memory_size_bytes": self.memory_size_bytes,
            "columns": {k: v.to_dict() for k, v in self.columns.items()},
        }
    
    def summary(self) -> str:
        """Generate human-readable summary."""
        lines = [
            f"Data Profile: {self.name}",
            f"=" * 50,
            f"Rows: {self.row_count:,}",
            f"Columns: {self.column_count}",
            f"Duplicates: {self.duplicate_row_count:,} ({self.duplicate_rate:.2%})",
            f"Overall Completeness: {self.overall_completeness:.2%}",
            "",
            "Column Summary:",
            "-" * 50,
        ]
        
        for col_name, col_profile in self.columns.items():
            lines.append(
                f"  {col_name}: {col_profile.data_type} | "
                f"Null: {col_profile.null_rate:.1%} | "
                f"Unique: {col_profile.unique_count:,}"
            )
        
        return "\n".join(lines)


class DataProfiler:
    """
    Profile datasets to understand data quality and characteristics.
    """
    
    def __init__(self, name: str = "DataProfiler"):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    def profile(
        self,
        data: List[Dict[str, Any]],
        name: str = "dataset",
        sample_size: Optional[int] = None,
    ) -> DataProfile:
        """
        Profile a dataset.
        
        Args:
            data: List of records to profile
            name: Name for the profile
            sample_size: Optional sample size for large datasets
        
        Returns:
            DataProfile with statistics
        """
        if not data:
            return DataProfile(name=name)
        
        # Sample if needed
        if sample_size and len(data) > sample_size:
            import random
            data = random.sample(data, sample_size)
            self.logger.info(f"Sampled {sample_size} records from {len(data)}")
        
        profile = DataProfile(
            name=name,
            row_count=len(data),
        )
        
        # Get all columns
        all_columns = set()
        for record in data:
            all_columns.update(record.keys())
        
        profile.column_count = len(all_columns)
        
        # Profile each column
        for column in all_columns:
            col_profile = self._profile_column(data, column)
            profile.columns[column] = col_profile
        
        # Check for duplicate rows
        profile.duplicate_row_count = self._count_duplicates(data)
        
        # Estimate memory size
        profile.memory_size_bytes = self._estimate_size(data)
        
        return profile
    
    def _profile_column(
        self,
        data: List[Dict[str, Any]],
        column: str,
    ) -> ColumnProfile:
        """Profile a single column."""
        profile = ColumnProfile(column_name=column, total_count=len(data))
        
        values = [record.get(column) for record in data]
        non_null_values = [v for v in values if v is not None]
        
        profile.null_count = len(values) - len(non_null_values)
        profile.unique_count = len(set(str(v) for v in non_null_values))
        
        if not non_null_values:
            profile.data_type = "null"
            return profile
        
        # Determine data type
        profile.data_type = self._infer_type(non_null_values)
        
        # Type-specific statistics
        if profile.data_type in ["int", "float"]:
            self._profile_numeric(profile, non_null_values)
        elif profile.data_type == "string":
            self._profile_string(profile, non_null_values)
        
        # Top values
        value_counts = Counter(str(v) for v in non_null_values)
        profile.top_values = value_counts.most_common(10)
        
        return profile
    
    def _infer_type(self, values: List[Any]) -> str:
        """Infer the data type of values."""
        sample = values[:100]  # Sample for efficiency
        
        type_counts = Counter()
        for v in sample:
            if isinstance(v, bool):
                type_counts["bool"] += 1
            elif isinstance(v, int):
                type_counts["int"] += 1
            elif isinstance(v, float):
                type_counts["float"] += 1
            elif isinstance(v, str):
                # Try to detect date/datetime strings
                if self._is_date_string(v):
                    type_counts["date"] += 1
                else:
                    type_counts["string"] += 1
            elif isinstance(v, (list, tuple)):
                type_counts["array"] += 1
            elif isinstance(v, dict):
                type_counts["object"] += 1
            else:
                type_counts["other"] += 1
        
        if type_counts:
            return type_counts.most_common(1)[0][0]
        return "unknown"
    
    def _is_date_string(self, value: str) -> bool:
        """Check if string looks like a date."""
        date_patterns = [
            r"^\d{4}-\d{2}-\d{2}",  # ISO date
            r"^\d{2}/\d{2}/\d{4}",  # US date
            r"^\d{4}-\d{2}-\d{2}T",  # ISO datetime
        ]
        import re
        return any(re.match(p, value) for p in date_patterns)
    
    def _profile_numeric(self, profile: ColumnProfile, values: List[Any]):
        """Add numeric statistics to profile."""
        try:
            numeric_values = [float(v) for v in values if v is not None]
            
            if numeric_values:
                profile.min_value = min(numeric_values)
                profile.max_value = max(numeric_values)
                profile.mean_value = statistics.mean(numeric_values)
                profile.sum_value = sum(numeric_values)
                
                if len(numeric_values) > 1:
                    profile.median_value = statistics.median(numeric_values)
                    profile.std_dev = statistics.stdev(numeric_values)
        except (ValueError, TypeError):
            pass
    
    def _profile_string(self, profile: ColumnProfile, values: List[Any]):
        """Add string statistics to profile."""
        try:
            string_values = [str(v) for v in values if v is not None]
            lengths = [len(s) for s in string_values]
            
            if lengths:
                profile.min_length = min(lengths)
                profile.max_length = max(lengths)
                profile.avg_length = statistics.mean(lengths)
        except (ValueError, TypeError):
            pass
    
    def _count_duplicates(self, data: List[Dict[str, Any]]) -> int:
        """Count duplicate rows."""
        seen = set()
        duplicates = 0
        
        for record in data:
            # Create hashable representation
            key = tuple(sorted((k, str(v)) for k, v in record.items()))
            if key in seen:
                duplicates += 1
            else:
                seen.add(key)
        
        return duplicates
    
    def _estimate_size(self, data: List[Dict[str, Any]]) -> int:
        """Estimate memory size of data."""
        import sys
        
        # Rough estimation
        if not data:
            return 0
        
        sample_record = data[0]
        record_size = sys.getsizeof(sample_record)
        for k, v in sample_record.items():
            record_size += sys.getsizeof(k) + sys.getsizeof(v)
        
        return record_size * len(data)
    
    def compare_profiles(
        self,
        profile1: DataProfile,
        profile2: DataProfile,
    ) -> Dict[str, Any]:
        """
        Compare two profiles to detect drift or changes.
        
        Useful for comparing:
        - Source vs Target data
        - Historical vs Current data
        - Expected vs Actual data
        """
        comparison = {
            "profile1_name": profile1.name,
            "profile2_name": profile2.name,
            "row_count_diff": profile2.row_count - profile1.row_count,
            "row_count_change_pct": (
                (profile2.row_count - profile1.row_count) / profile1.row_count * 100
                if profile1.row_count > 0 else 0
            ),
            "completeness_diff": profile2.overall_completeness - profile1.overall_completeness,
            "column_changes": {
                "added": [],
                "removed": [],
                "modified": [],
            },
        }
        
        cols1 = set(profile1.columns.keys())
        cols2 = set(profile2.columns.keys())
        
        comparison["column_changes"]["added"] = list(cols2 - cols1)
        comparison["column_changes"]["removed"] = list(cols1 - cols2)
        
        # Compare common columns
        for col in cols1 & cols2:
            p1 = profile1.columns[col]
            p2 = profile2.columns[col]
            
            changes = {}
            
            if abs(p1.null_rate - p2.null_rate) > 0.05:
                changes["null_rate"] = {
                    "before": p1.null_rate,
                    "after": p2.null_rate,
                }
            
            if p1.data_type != p2.data_type:
                changes["data_type"] = {
                    "before": p1.data_type,
                    "after": p2.data_type,
                }
            
            if p1.unique_count != p2.unique_count:
                changes["unique_count"] = {
                    "before": p1.unique_count,
                    "after": p2.unique_count,
                }
            
            if changes:
                comparison["column_changes"]["modified"].append({
                    "column": col,
                    "changes": changes,
                })
        
        return comparison


class DataQualityScorer:
    """Calculate overall data quality scores."""
    
    def __init__(
        self,
        completeness_weight: float = 0.3,
        uniqueness_weight: float = 0.2,
        validity_weight: float = 0.3,
        consistency_weight: float = 0.2,
    ):
        self.weights = {
            "completeness": completeness_weight,
            "uniqueness": uniqueness_weight,
            "validity": validity_weight,
            "consistency": consistency_weight,
        }
    
    def score(
        self,
        profile: DataProfile,
        validation_report: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Calculate data quality score.
        
        Returns score from 0-100 with breakdown.
        """
        scores = {}
        
        # Completeness score (based on null rates)
        if profile.columns:
            completeness_scores = [
                col.completeness for col in profile.columns.values()
            ]
            scores["completeness"] = sum(completeness_scores) / len(completeness_scores) * 100
        else:
            scores["completeness"] = 0
        
        # Uniqueness score (for key columns, assume first column is key)
        if profile.columns:
            first_col = list(profile.columns.values())[0]
            scores["uniqueness"] = first_col.uniqueness_rate * 100
        else:
            scores["uniqueness"] = 0
        
        # Validity score (from validation report if provided)
        if validation_report:
            scores["validity"] = validation_report.validity_rate * 100
        else:
            scores["validity"] = 100  # Assume valid if no validation
        
        # Consistency score (based on duplicate rate)
        scores["consistency"] = (1 - profile.duplicate_rate) * 100
        
        # Calculate weighted overall score
        overall = sum(
            scores[dim] * self.weights[dim]
            for dim in self.weights
        )
        
        return {
            "overall_score": round(overall, 2),
            "dimension_scores": {k: round(v, 2) for k, v in scores.items()},
            "weights": self.weights,
            "grade": self._score_to_grade(overall),
        }
    
    def _score_to_grade(self, score: float) -> str:
        """Convert numeric score to letter grade."""
        if score >= 95:
            return "A+"
        elif score >= 90:
            return "A"
        elif score >= 85:
            return "B+"
        elif score >= 80:
            return "B"
        elif score >= 75:
            return "C+"
        elif score >= 70:
            return "C"
        elif score >= 60:
            return "D"
        else:
            return "F"
