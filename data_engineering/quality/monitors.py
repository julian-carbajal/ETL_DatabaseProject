"""
Data Quality Monitoring and Alerting.
Provides continuous monitoring and alerting capabilities.
"""

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import logging

from .profiler import DataProfile, DataProfiler
from .validators import ValidationReport

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


@dataclass
class Alert:
    """Data quality alert."""
    alert_id: str
    severity: AlertSeverity
    title: str
    message: str
    metric_name: str
    metric_value: Any
    threshold: Any
    created_at: datetime = field(default_factory=datetime.utcnow)
    acknowledged: bool = False
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "threshold": self.threshold,
            "created_at": self.created_at.isoformat(),
            "acknowledged": self.acknowledged,
        }


@dataclass
class MonitoringRule:
    """Rule for monitoring data quality metrics."""
    name: str
    metric: str
    operator: str  # gt, lt, eq, gte, lte, ne
    threshold: Any
    severity: AlertSeverity = AlertSeverity.MEDIUM
    description: str = ""
    enabled: bool = True
    
    def evaluate(self, value: Any) -> bool:
        """Evaluate if rule is violated. Returns True if violated."""
        try:
            if self.operator == "gt":
                return value > self.threshold
            elif self.operator == "lt":
                return value < self.threshold
            elif self.operator == "gte":
                return value >= self.threshold
            elif self.operator == "lte":
                return value <= self.threshold
            elif self.operator == "eq":
                return value == self.threshold
            elif self.operator == "ne":
                return value != self.threshold
            return False
        except (TypeError, ValueError):
            return False


class DataQualityMonitor:
    """
    Monitor data quality metrics and generate alerts.
    """
    
    def __init__(
        self,
        name: str = "DataQualityMonitor",
        history_file: Optional[str] = None,
    ):
        self.name = name
        self.rules: List[MonitoringRule] = []
        self.alerts: List[Alert] = []
        self.history: List[Dict[str, Any]] = []
        self.history_file = Path(history_file) if history_file else None
        self.profiler = DataProfiler()
        self.logger = logging.getLogger(f"{__name__}.{name}")
        
        self._alert_counter = 0
        
        # Load history if exists
        if self.history_file and self.history_file.exists():
            self._load_history()
    
    def add_rule(self, rule: MonitoringRule) -> "DataQualityMonitor":
        """Add a monitoring rule."""
        self.rules.append(rule)
        return self
    
    def add_completeness_rule(
        self,
        column: str,
        min_completeness: float = 0.95,
        severity: AlertSeverity = AlertSeverity.HIGH,
    ) -> "DataQualityMonitor":
        """Add rule for column completeness."""
        return self.add_rule(MonitoringRule(
            name=f"completeness_{column}",
            metric=f"columns.{column}.completeness",
            operator="lt",
            threshold=min_completeness,
            severity=severity,
            description=f"Column {column} completeness below {min_completeness:.0%}",
        ))
    
    def add_uniqueness_rule(
        self,
        column: str,
        min_uniqueness: float = 1.0,
        severity: AlertSeverity = AlertSeverity.HIGH,
    ) -> "DataQualityMonitor":
        """Add rule for column uniqueness."""
        return self.add_rule(MonitoringRule(
            name=f"uniqueness_{column}",
            metric=f"columns.{column}.uniqueness_rate",
            operator="lt",
            threshold=min_uniqueness,
            severity=severity,
            description=f"Column {column} uniqueness below {min_uniqueness:.0%}",
        ))
    
    def add_row_count_rule(
        self,
        min_rows: int = 0,
        max_rows: Optional[int] = None,
        severity: AlertSeverity = AlertSeverity.MEDIUM,
    ) -> "DataQualityMonitor":
        """Add rule for row count bounds."""
        if min_rows > 0:
            self.add_rule(MonitoringRule(
                name="min_row_count",
                metric="row_count",
                operator="lt",
                threshold=min_rows,
                severity=severity,
                description=f"Row count below minimum {min_rows}",
            ))
        if max_rows:
            self.add_rule(MonitoringRule(
                name="max_row_count",
                metric="row_count",
                operator="gt",
                threshold=max_rows,
                severity=severity,
                description=f"Row count exceeds maximum {max_rows}",
            ))
        return self
    
    def add_freshness_rule(
        self,
        max_age_hours: int = 24,
        severity: AlertSeverity = AlertSeverity.HIGH,
    ) -> "DataQualityMonitor":
        """Add rule for data freshness."""
        return self.add_rule(MonitoringRule(
            name="data_freshness",
            metric="age_hours",
            operator="gt",
            threshold=max_age_hours,
            severity=severity,
            description=f"Data older than {max_age_hours} hours",
        ))
    
    def monitor(
        self,
        data: List[Dict[str, Any]],
        name: str = "dataset",
        timestamp_column: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Monitor data quality and generate alerts.
        
        Args:
            data: Data to monitor
            name: Dataset name
            timestamp_column: Column containing timestamps for freshness check
        
        Returns:
            Monitoring result with profile and alerts
        """
        # Profile the data
        profile = self.profiler.profile(data, name)
        profile_dict = profile.to_dict()
        
        # Calculate freshness if timestamp column provided
        if timestamp_column:
            profile_dict["age_hours"] = self._calculate_age(data, timestamp_column)
        
        # Evaluate rules
        new_alerts = []
        for rule in self.rules:
            if not rule.enabled:
                continue
            
            value = self._get_metric_value(profile_dict, rule.metric)
            if value is not None and rule.evaluate(value):
                alert = self._create_alert(rule, value)
                new_alerts.append(alert)
                self.alerts.append(alert)
        
        # Store in history
        result = {
            "timestamp": datetime.utcnow().isoformat(),
            "dataset": name,
            "profile": profile_dict,
            "alerts": [a.to_dict() for a in new_alerts],
            "alert_count": len(new_alerts),
        }
        self.history.append(result)
        
        # Save history
        if self.history_file:
            self._save_history()
        
        # Log alerts
        for alert in new_alerts:
            self.logger.warning(
                f"[{alert.severity.value}] {alert.title}: {alert.message}"
            )
        
        return result
    
    def _get_metric_value(self, profile_dict: Dict[str, Any], metric: str) -> Any:
        """Extract metric value from profile dictionary."""
        parts = metric.split(".")
        value = profile_dict
        
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None
        
        return value
    
    def _calculate_age(self, data: List[Dict[str, Any]], timestamp_column: str) -> float:
        """Calculate data age in hours."""
        if not data:
            return float("inf")
        
        timestamps = []
        for record in data:
            ts = record.get(timestamp_column)
            if ts:
                if isinstance(ts, str):
                    try:
                        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    except ValueError:
                        continue
                if isinstance(ts, datetime):
                    timestamps.append(ts)
        
        if not timestamps:
            return float("inf")
        
        max_ts = max(timestamps)
        age = datetime.utcnow() - max_ts.replace(tzinfo=None)
        return age.total_seconds() / 3600
    
    def _create_alert(self, rule: MonitoringRule, value: Any) -> Alert:
        """Create alert from rule violation."""
        self._alert_counter += 1
        
        return Alert(
            alert_id=f"ALT-{datetime.utcnow().strftime('%Y%m%d')}-{self._alert_counter:04d}",
            severity=rule.severity,
            title=rule.name,
            message=rule.description or f"{rule.metric} {rule.operator} {rule.threshold}",
            metric_name=rule.metric,
            metric_value=value,
            threshold=rule.threshold,
        )
    
    def get_active_alerts(
        self,
        severity: Optional[AlertSeverity] = None,
    ) -> List[Alert]:
        """Get unacknowledged alerts."""
        alerts = [a for a in self.alerts if not a.acknowledged]
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        return alerts
    
    def acknowledge_alert(self, alert_id: str, acknowledged_by: str):
        """Acknowledge an alert."""
        for alert in self.alerts:
            if alert.alert_id == alert_id:
                alert.acknowledged = True
                alert.acknowledged_by = acknowledged_by
                alert.acknowledged_at = datetime.utcnow()
                break
    
    def get_trend(
        self,
        metric: str,
        periods: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get trend data for a metric."""
        trend = []
        for entry in self.history[-periods:]:
            value = self._get_metric_value(entry.get("profile", {}), metric)
            trend.append({
                "timestamp": entry.get("timestamp"),
                "value": value,
            })
        return trend
    
    def _save_history(self):
        """Save monitoring history to file."""
        if not self.history_file:
            return
        
        self.history_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Keep last 1000 entries
        history_to_save = self.history[-1000:]
        
        with open(self.history_file, "w") as f:
            json.dump(history_to_save, f, indent=2, default=str)
    
    def _load_history(self):
        """Load monitoring history from file."""
        if not self.history_file or not self.history_file.exists():
            return
        
        try:
            with open(self.history_file, "r") as f:
                self.history = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            self.logger.warning(f"Failed to load history: {e}")


class AlertManager:
    """
    Manage and route alerts to different channels.
    """
    
    def __init__(self):
        self.handlers: Dict[AlertSeverity, List[Callable[[Alert], None]]] = {
            severity: [] for severity in AlertSeverity
        }
        self.logger = logging.getLogger(f"{__name__}.AlertManager")
    
    def add_handler(
        self,
        handler: Callable[[Alert], None],
        severities: List[AlertSeverity] = None,
    ):
        """Add alert handler for specific severities."""
        if severities is None:
            severities = list(AlertSeverity)
        
        for severity in severities:
            self.handlers[severity].append(handler)
    
    def handle_alert(self, alert: Alert):
        """Route alert to appropriate handlers."""
        handlers = self.handlers.get(alert.severity, [])
        
        for handler in handlers:
            try:
                handler(alert)
            except Exception as e:
                self.logger.error(f"Alert handler failed: {e}")
    
    def handle_alerts(self, alerts: List[Alert]):
        """Handle multiple alerts."""
        for alert in alerts:
            self.handle_alert(alert)


# Pre-built alert handlers
def log_alert_handler(alert: Alert):
    """Log alert to standard logging."""
    logger.warning(
        f"[{alert.severity.value}] {alert.title}: {alert.message} "
        f"(metric={alert.metric_name}, value={alert.metric_value}, threshold={alert.threshold})"
    )


def file_alert_handler(file_path: str) -> Callable[[Alert], None]:
    """Create handler that writes alerts to file."""
    def handler(alert: Alert):
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, "a") as f:
            f.write(json.dumps(alert.to_dict()) + "\n")
    
    return handler


def console_alert_handler(alert: Alert):
    """Print alert to console with formatting."""
    severity_colors = {
        AlertSeverity.CRITICAL: "\033[91m",  # Red
        AlertSeverity.HIGH: "\033[93m",      # Yellow
        AlertSeverity.MEDIUM: "\033[94m",    # Blue
        AlertSeverity.LOW: "\033[92m",       # Green
        AlertSeverity.INFO: "\033[97m",      # White
    }
    reset = "\033[0m"
    
    color = severity_colors.get(alert.severity, "")
    print(f"{color}[{alert.severity.value}] {alert.title}{reset}")
    print(f"  Message: {alert.message}")
    print(f"  Metric: {alert.metric_name} = {alert.metric_value} (threshold: {alert.threshold})")
    print(f"  Time: {alert.created_at.isoformat()}")
    print()
