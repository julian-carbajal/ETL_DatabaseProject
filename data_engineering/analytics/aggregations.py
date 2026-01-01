"""
Data Aggregation and Metrics Calculation.
Provides analytics capabilities across all domains.
"""

from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from collections import defaultdict
import statistics
import logging

logger = logging.getLogger(__name__)


@dataclass
class AggregationResult:
    """Result of an aggregation operation."""
    name: str
    dimensions: List[str]
    metrics: Dict[str, Any]
    row_count: int
    data: List[Dict[str, Any]]
    computed_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "dimensions": self.dimensions,
            "metrics": self.metrics,
            "row_count": self.row_count,
            "computed_at": self.computed_at.isoformat(),
            "data": self.data,
        }


class DataAggregator:
    """
    Aggregate data with flexible grouping and metrics.
    """
    
    def __init__(self, name: str = "DataAggregator"):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    def aggregate(
        self,
        data: List[Dict[str, Any]],
        group_by: List[str],
        metrics: Dict[str, Dict[str, str]],
        filters: Dict[str, Any] = None,
        having: Dict[str, Any] = None,
    ) -> AggregationResult:
        """
        Aggregate data with grouping and metrics.
        
        Args:
            data: Source data
            group_by: Columns to group by
            metrics: {metric_name: {"column": col, "function": func}}
                     Functions: sum, avg, count, min, max, count_distinct, first, last
            filters: Pre-aggregation filters {column: value or callable}
            having: Post-aggregation filters {metric: {"operator": op, "value": val}}
        
        Returns:
            AggregationResult with grouped data
        """
        # Apply pre-aggregation filters
        if filters:
            data = self._apply_filters(data, filters)
        
        # Group data
        groups = defaultdict(list)
        for record in data:
            key = tuple(record.get(col) for col in group_by)
            groups[key].append(record)
        
        # Calculate metrics for each group
        results = []
        for key, group_records in groups.items():
            row = dict(zip(group_by, key))
            
            for metric_name, metric_config in metrics.items():
                column = metric_config.get("column")
                func = metric_config.get("function", "count")
                
                values = [r.get(column) for r in group_records if r.get(column) is not None]
                row[metric_name] = self._calculate_metric(values, func, group_records)
            
            results.append(row)
        
        # Apply having filters
        if having:
            results = self._apply_having(results, having)
        
        return AggregationResult(
            name=f"aggregation_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            dimensions=group_by,
            metrics=list(metrics.keys()),
            row_count=len(results),
            data=results,
        )
    
    def _apply_filters(
        self,
        data: List[Dict[str, Any]],
        filters: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Apply pre-aggregation filters."""
        filtered = []
        
        for record in data:
            include = True
            for column, condition in filters.items():
                value = record.get(column)
                
                if callable(condition):
                    if not condition(value):
                        include = False
                        break
                elif isinstance(condition, list):
                    if value not in condition:
                        include = False
                        break
                else:
                    if value != condition:
                        include = False
                        break
            
            if include:
                filtered.append(record)
        
        return filtered
    
    def _calculate_metric(
        self,
        values: List[Any],
        func: str,
        records: List[Dict[str, Any]] = None,
    ) -> Any:
        """Calculate a single metric."""
        if func == "count":
            return len(records) if records else len(values)
        elif func == "count_distinct":
            return len(set(values))
        elif func == "sum":
            try:
                return sum(float(v) for v in values if v is not None)
            except (ValueError, TypeError):
                return None
        elif func == "avg":
            try:
                numeric = [float(v) for v in values if v is not None]
                return statistics.mean(numeric) if numeric else None
            except (ValueError, TypeError):
                return None
        elif func == "min":
            try:
                return min(values) if values else None
            except (ValueError, TypeError):
                return None
        elif func == "max":
            try:
                return max(values) if values else None
            except (ValueError, TypeError):
                return None
        elif func == "first":
            return values[0] if values else None
        elif func == "last":
            return values[-1] if values else None
        elif func == "std":
            try:
                numeric = [float(v) for v in values if v is not None]
                return statistics.stdev(numeric) if len(numeric) > 1 else None
            except (ValueError, TypeError):
                return None
        
        return None
    
    def _apply_having(
        self,
        data: List[Dict[str, Any]],
        having: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Apply post-aggregation filters."""
        filtered = []
        
        for record in data:
            include = True
            for metric, condition in having.items():
                value = record.get(metric)
                operator = condition.get("operator", "eq")
                threshold = condition.get("value")
                
                if value is None:
                    include = False
                    break
                
                if operator == "gt" and not (value > threshold):
                    include = False
                elif operator == "lt" and not (value < threshold):
                    include = False
                elif operator == "gte" and not (value >= threshold):
                    include = False
                elif operator == "lte" and not (value <= threshold):
                    include = False
                elif operator == "eq" and not (value == threshold):
                    include = False
                elif operator == "ne" and not (value != threshold):
                    include = False
                
                if not include:
                    break
            
            if include:
                filtered.append(record)
        
        return filtered
    
    def pivot(
        self,
        data: List[Dict[str, Any]],
        index: List[str],
        columns: str,
        values: str,
        aggfunc: str = "sum",
    ) -> List[Dict[str, Any]]:
        """
        Create pivot table from data.
        
        Args:
            data: Source data
            index: Row grouping columns
            columns: Column to pivot
            values: Values to aggregate
            aggfunc: Aggregation function
        
        Returns:
            Pivoted data
        """
        # Get unique column values
        column_values = sorted(set(r.get(columns) for r in data if r.get(columns) is not None))
        
        # Group by index
        groups = defaultdict(lambda: defaultdict(list))
        for record in data:
            idx_key = tuple(record.get(col) for col in index)
            col_val = record.get(columns)
            val = record.get(values)
            
            if col_val is not None and val is not None:
                groups[idx_key][col_val].append(val)
        
        # Build pivot result
        results = []
        for idx_key, col_data in groups.items():
            row = dict(zip(index, idx_key))
            
            for col_val in column_values:
                values_list = col_data.get(col_val, [])
                row[f"{columns}_{col_val}"] = self._calculate_metric(values_list, aggfunc)
            
            results.append(row)
        
        return results
    
    def window(
        self,
        data: List[Dict[str, Any]],
        partition_by: List[str],
        order_by: str,
        metrics: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Calculate window functions.
        
        Args:
            data: Source data
            partition_by: Columns to partition by
            order_by: Column to order by within partition
            metrics: {metric_name: {"function": func, "column": col, "window": n}}
                     Functions: row_number, rank, lag, lead, running_sum, running_avg
        
        Returns:
            Data with window metrics added
        """
        # Group by partition
        partitions = defaultdict(list)
        for record in data:
            key = tuple(record.get(col) for col in partition_by)
            partitions[key].append(record)
        
        results = []
        
        for partition_key, partition_data in partitions.items():
            # Sort partition
            sorted_partition = sorted(
                partition_data,
                key=lambda x: x.get(order_by) or ""
            )
            
            # Calculate window metrics
            for i, record in enumerate(sorted_partition):
                new_record = record.copy()
                
                for metric_name, metric_config in metrics.items():
                    func = metric_config.get("function")
                    column = metric_config.get("column")
                    window = metric_config.get("window", 1)
                    
                    if func == "row_number":
                        new_record[metric_name] = i + 1
                    elif func == "rank":
                        # Simple rank (doesn't handle ties)
                        new_record[metric_name] = i + 1
                    elif func == "lag":
                        if i >= window:
                            new_record[metric_name] = sorted_partition[i - window].get(column)
                        else:
                            new_record[metric_name] = None
                    elif func == "lead":
                        if i + window < len(sorted_partition):
                            new_record[metric_name] = sorted_partition[i + window].get(column)
                        else:
                            new_record[metric_name] = None
                    elif func == "running_sum":
                        values = [r.get(column) for r in sorted_partition[:i+1] if r.get(column) is not None]
                        try:
                            new_record[metric_name] = sum(float(v) for v in values)
                        except (ValueError, TypeError):
                            new_record[metric_name] = None
                    elif func == "running_avg":
                        values = [r.get(column) for r in sorted_partition[:i+1] if r.get(column) is not None]
                        try:
                            numeric = [float(v) for v in values]
                            new_record[metric_name] = statistics.mean(numeric) if numeric else None
                        except (ValueError, TypeError):
                            new_record[metric_name] = None
                
                results.append(new_record)
        
        return results


class MetricsCalculator:
    """
    Calculate domain-specific metrics and KPIs.
    """
    
    def __init__(self):
        self.aggregator = DataAggregator()
    
    # Health Domain Metrics
    def calculate_health_metrics(self, data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Calculate healthcare KPIs."""
        metrics = {}
        
        # Patient metrics
        if "patients" in data:
            patients = data["patients"]
            metrics["total_patients"] = len(patients)
            metrics["active_patients"] = sum(1 for p in patients if not p.get("is_deceased"))
            
            # Age distribution
            ages = []
            for p in patients:
                dob = p.get("date_of_birth")
                if dob:
                    if isinstance(dob, str):
                        dob = datetime.fromisoformat(dob).date()
                    age = (date.today() - dob).days // 365
                    ages.append(age)
            
            if ages:
                metrics["avg_patient_age"] = round(statistics.mean(ages), 1)
                metrics["patient_age_std"] = round(statistics.stdev(ages), 1) if len(ages) > 1 else 0
        
        # Encounter metrics
        if "encounters" in data:
            encounters = data["encounters"]
            metrics["total_encounters"] = len(encounters)
            
            # Encounter type distribution
            type_counts = defaultdict(int)
            for e in encounters:
                type_counts[e.get("encounter_type", "UNKNOWN")] += 1
            metrics["encounters_by_type"] = dict(type_counts)
            
            # Average charges
            charges = [e.get("total_charges") for e in encounters if e.get("total_charges")]
            if charges:
                metrics["avg_encounter_charge"] = round(statistics.mean(charges), 2)
        
        # Lab result metrics
        if "lab_results" in data:
            labs = data["lab_results"]
            metrics["total_lab_results"] = len(labs)
            
            abnormal = sum(1 for l in labs if l.get("abnormal_flag"))
            metrics["abnormal_lab_rate"] = round(abnormal / len(labs), 4) if labs else 0
            
            critical = sum(1 for l in labs if l.get("critical_flag"))
            metrics["critical_lab_rate"] = round(critical / len(labs), 4) if labs else 0
        
        return metrics
    
    # Finance Domain Metrics
    def calculate_finance_metrics(self, data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Calculate financial KPIs."""
        metrics = {}
        
        # Account metrics
        if "accounts" in data:
            accounts = data["accounts"]
            metrics["total_accounts"] = len(accounts)
            
            # Balance metrics
            balances = [a.get("current_balance", 0) for a in accounts]
            metrics["total_balance"] = round(sum(balances), 2)
            metrics["avg_balance"] = round(statistics.mean(balances), 2) if balances else 0
            
            # Account type distribution
            type_counts = defaultdict(int)
            for a in accounts:
                type_counts[a.get("account_type", "UNKNOWN")] += 1
            metrics["accounts_by_type"] = dict(type_counts)
            
            # Risk metrics
            risk_scores = [a.get("aml_risk_score") for a in accounts if a.get("aml_risk_score") is not None]
            if risk_scores:
                metrics["avg_aml_risk_score"] = round(statistics.mean(risk_scores), 2)
                metrics["high_risk_accounts"] = sum(1 for s in risk_scores if s > 50)
        
        # Transaction metrics
        if "transactions" in data:
            txns = data["transactions"]
            metrics["total_transactions"] = len(txns)
            
            # Volume metrics
            amounts = [abs(t.get("amount", 0)) for t in txns]
            metrics["total_transaction_volume"] = round(sum(amounts), 2)
            metrics["avg_transaction_amount"] = round(statistics.mean(amounts), 2) if amounts else 0
            
            # Fraud metrics
            flagged = sum(1 for t in txns if t.get("is_flagged"))
            metrics["flagged_transaction_rate"] = round(flagged / len(txns), 4) if txns else 0
        
        # Loan metrics
        if "loan_applications" in data:
            loans = data["loan_applications"]
            metrics["total_loan_applications"] = len(loans)
            
            approved = sum(1 for l in loans if l.get("status") == "APPROVED")
            metrics["loan_approval_rate"] = round(approved / len(loans), 4) if loans else 0
            
            amounts = [l.get("requested_amount", 0) for l in loans]
            metrics["total_loan_volume_requested"] = round(sum(amounts), 2)
        
        return metrics
    
    # Tech Domain Metrics
    def calculate_tech_metrics(self, data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Calculate infrastructure and application KPIs."""
        metrics = {}
        
        # Server metrics
        if "servers" in data:
            servers = data["servers"]
            metrics["total_servers"] = len(servers)
            
            # Status distribution
            status_counts = defaultdict(int)
            for s in servers:
                status_counts[s.get("status", "UNKNOWN")] += 1
            metrics["servers_by_status"] = dict(status_counts)
            
            # Environment distribution
            env_counts = defaultdict(int)
            for s in servers:
                env_counts[s.get("environment", "UNKNOWN")] += 1
            metrics["servers_by_environment"] = dict(env_counts)
            
            # Running rate
            running = sum(1 for s in servers if s.get("status") == "RUNNING")
            metrics["server_availability_rate"] = round(running / len(servers), 4) if servers else 0
        
        # Incident metrics
        if "incidents" in data:
            incidents = data["incidents"]
            metrics["total_incidents"] = len(incidents)
            
            # Severity distribution
            sev_counts = defaultdict(int)
            for i in incidents:
                sev_counts[i.get("severity", "UNKNOWN")] += 1
            metrics["incidents_by_severity"] = dict(sev_counts)
            
            # MTTR (Mean Time To Resolve)
            resolve_times = [i.get("time_to_resolve_minutes") for i in incidents 
                           if i.get("time_to_resolve_minutes") is not None]
            if resolve_times:
                metrics["mttr_minutes"] = round(statistics.mean(resolve_times), 2)
            
            # Resolution rate
            resolved = sum(1 for i in incidents if i.get("status") in ["RESOLVED", "CLOSED"])
            metrics["incident_resolution_rate"] = round(resolved / len(incidents), 4) if incidents else 0
        
        # Deployment metrics
        if "deployments" in data:
            deployments = data["deployments"]
            metrics["total_deployments"] = len(deployments)
            
            successful = sum(1 for d in deployments if d.get("status") == "SUCCESS")
            metrics["deployment_success_rate"] = round(successful / len(deployments), 4) if deployments else 0
            
            durations = [d.get("duration_seconds") for d in deployments 
                        if d.get("duration_seconds") is not None]
            if durations:
                metrics["avg_deployment_duration_seconds"] = round(statistics.mean(durations), 2)
        
        # Metrics data
        if "metrics" in data:
            metric_data = data["metrics"]
            
            cpu_values = [m.get("cpu_usage_pct") for m in metric_data if m.get("cpu_usage_pct") is not None]
            if cpu_values:
                metrics["avg_cpu_usage"] = round(statistics.mean(cpu_values), 2)
                metrics["max_cpu_usage"] = round(max(cpu_values), 2)
            
            mem_values = [m.get("memory_used_pct") for m in metric_data if m.get("memory_used_pct") is not None]
            if mem_values:
                metrics["avg_memory_usage"] = round(statistics.mean(mem_values), 2)
                metrics["max_memory_usage"] = round(max(mem_values), 2)
        
        return metrics
    
    # University Domain Metrics
    def calculate_university_metrics(self, data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Calculate academic KPIs."""
        metrics = {}
        
        # Student metrics
        if "students" in data:
            students = data["students"]
            metrics["total_students"] = len(students)
            
            # Enrollment status
            status_counts = defaultdict(int)
            for s in students:
                status_counts[s.get("enrollment_status", "UNKNOWN")] += 1
            metrics["students_by_status"] = dict(status_counts)
            
            # Academic level distribution
            level_counts = defaultdict(int)
            for s in students:
                level_counts[s.get("academic_level", "UNKNOWN")] += 1
            metrics["students_by_level"] = dict(level_counts)
            
            # GPA metrics
            gpas = [s.get("gpa") for s in students if s.get("gpa") is not None]
            if gpas:
                metrics["avg_gpa"] = round(statistics.mean(gpas), 2)
                metrics["gpa_std"] = round(statistics.stdev(gpas), 2) if len(gpas) > 1 else 0
                metrics["honors_students"] = sum(1 for g in gpas if g >= 3.5)
            
            # Retention (active students)
            active = sum(1 for s in students if s.get("enrollment_status") == "ACTIVE")
            metrics["retention_rate"] = round(active / len(students), 4) if students else 0
        
        # Faculty metrics
        if "faculty" in data:
            faculty = data["faculty"]
            metrics["total_faculty"] = len(faculty)
            
            # Rank distribution
            rank_counts = defaultdict(int)
            for f in faculty:
                rank_counts[f.get("rank", "UNKNOWN")] += 1
            metrics["faculty_by_rank"] = dict(rank_counts)
            
            # Publication metrics
            pubs = [f.get("publications_count", 0) for f in faculty]
            metrics["total_publications"] = sum(pubs)
            metrics["avg_publications_per_faculty"] = round(statistics.mean(pubs), 2) if pubs else 0
        
        # Course metrics
        if "courses" in data:
            courses = data["courses"]
            metrics["total_courses"] = len(courses)
            
            # Enrollment metrics
            enrolled = [c.get("enrolled_count", 0) for c in courses]
            capacity = [c.get("capacity", 1) for c in courses]
            
            metrics["total_course_enrollment"] = sum(enrolled)
            
            fill_rates = [e / c for e, c in zip(enrolled, capacity) if c > 0]
            if fill_rates:
                metrics["avg_course_fill_rate"] = round(statistics.mean(fill_rates), 4)
        
        # Enrollment metrics
        if "enrollments" in data:
            enrollments = data["enrollments"]
            metrics["total_enrollments"] = len(enrollments)
            
            # Grade distribution
            grades = [e.get("grade") for e in enrollments if e.get("grade")]
            if grades:
                grade_counts = defaultdict(int)
                for g in grades:
                    grade_counts[g] += 1
                metrics["grade_distribution"] = dict(grade_counts)
            
            # Pass rate
            completed = [e for e in enrollments if e.get("status") == "COMPLETED"]
            if completed:
                passed = sum(1 for e in completed if e.get("grade") not in ["F", None])
                metrics["pass_rate"] = round(passed / len(completed), 4)
        
        # Grant metrics
        if "research_grants" in data:
            grants = data["research_grants"]
            metrics["total_grants"] = len(grants)
            
            amounts = [g.get("total_amount", 0) for g in grants]
            metrics["total_grant_funding"] = round(sum(amounts), 2)
            
            active = sum(1 for g in grants if g.get("status") == "ACTIVE")
            metrics["active_grants"] = active
        
        return metrics
