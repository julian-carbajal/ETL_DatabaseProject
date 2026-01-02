"""
Event Consumer for Streaming Pipeline
======================================

Consumes and processes events from the message broker.
"""

import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import logging

from .stream_processor import (
    StreamProcessor, StreamConfig, StreamMessage,
    InMemoryMessageBroker, get_broker, WindowedAggregator
)

logger = logging.getLogger(__name__)


class EventConsumer:
    """
    Consumes events from topics and processes them.
    """
    
    def __init__(
        self,
        consumer_group: str = "default",
        broker: InMemoryMessageBroker = None,
    ):
        self.consumer_group = consumer_group
        self.broker = broker or get_broker()
        self.processors: Dict[str, StreamProcessor] = {}
        self.aggregators: Dict[str, WindowedAggregator] = {}
        
        self.logger = logging.getLogger(f"{__name__}.{consumer_group}")
    
    def create_processor(
        self,
        name: str,
        batch_size: int = 100,
        **kwargs,
    ) -> StreamProcessor:
        """Create a stream processor."""
        config = StreamConfig(
            name=name,
            batch_size=batch_size,
            **kwargs,
        )
        processor = StreamProcessor(config, self.broker)
        self.processors[name] = processor
        return processor
    
    def subscribe(
        self,
        topic: str,
        handler: Callable[[List[StreamMessage]], List[Dict[str, Any]]],
        processor_name: str = None,
    ):
        """Subscribe to a topic with a handler."""
        processor_name = processor_name or f"{topic}_processor"
        
        if processor_name not in self.processors:
            self.create_processor(processor_name)
        
        self.processors[processor_name].register_handler(topic, handler)
    
    def start_all(self):
        """Start all processors."""
        for name, processor in self.processors.items():
            processor.start(self.consumer_group)
            self.logger.info(f"Started processor: {name}")
    
    def stop_all(self):
        """Stop all processors."""
        for name, processor in self.processors.items():
            processor.stop()
            self.logger.info(f"Stopped processor: {name}")
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get metrics from all processors."""
        return {
            name: processor.get_metrics()
            for name, processor in self.processors.items()
        }


# =============================================================================
# Domain-Specific Event Handlers
# =============================================================================

class HealthEventHandler:
    """Handler for health domain events."""
    
    def __init__(self):
        self.encounter_count = 0
        self.abnormal_labs = 0
        self.critical_labs = 0
        self.vitals_aggregator = WindowedAggregator(window_size_seconds=60)
    
    def handle_encounters(self, messages: List[StreamMessage]) -> List[Dict[str, Any]]:
        """Process encounter events."""
        results = []
        
        for msg in messages:
            event = msg.value
            self.encounter_count += 1
            
            # Aggregate vitals
            vitals = event.get("vitals", {})
            if vitals:
                self.vitals_aggregator.add("heart_rate", vitals.get("heart_rate", 0))
                self.vitals_aggregator.add("bp_systolic", vitals.get("blood_pressure_systolic", 0))
            
            # Check for critical vitals
            alerts = []
            if vitals.get("heart_rate", 0) > 150:
                alerts.append("Tachycardia detected")
            if vitals.get("blood_pressure_systolic", 0) > 180:
                alerts.append("Hypertensive crisis")
            if vitals.get("temperature", 0) > 103:
                alerts.append("High fever")
            
            result = {
                "encounter_id": event.get("encounter_id"),
                "patient_id": event.get("patient_id"),
                "processed_at": datetime.utcnow().isoformat(),
                "alerts": alerts,
            }
            results.append(result)
        
        return results
    
    def handle_lab_results(self, messages: List[StreamMessage]) -> List[Dict[str, Any]]:
        """Process lab result events."""
        results = []
        
        for msg in messages:
            event = msg.value
            
            if event.get("abnormal_flag"):
                self.abnormal_labs += 1
            if event.get("critical_flag"):
                self.critical_labs += 1
            
            result = {
                "patient_id": event.get("patient_id"),
                "test_code": event.get("test_code"),
                "value": event.get("value"),
                "is_abnormal": event.get("abnormal_flag"),
                "is_critical": event.get("critical_flag"),
                "processed_at": datetime.utcnow().isoformat(),
            }
            results.append(result)
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get handler statistics."""
        return {
            "encounter_count": self.encounter_count,
            "abnormal_labs": self.abnormal_labs,
            "critical_labs": self.critical_labs,
            "abnormal_rate": self.abnormal_labs / max(1, self.encounter_count),
        }


class FinanceEventHandler:
    """Handler for finance domain events."""
    
    def __init__(self):
        self.transaction_count = 0
        self.total_volume = 0.0
        self.fraud_alerts = 0
        self.amount_aggregator = WindowedAggregator(window_size_seconds=60)
    
    def handle_transactions(self, messages: List[StreamMessage]) -> List[Dict[str, Any]]:
        """Process transaction events."""
        results = []
        
        for msg in messages:
            event = msg.value
            self.transaction_count += 1
            
            amount = abs(event.get("amount", 0))
            self.total_volume += amount
            self.amount_aggregator.add("transaction_amount", amount)
            
            # Enrich with risk assessment
            risk_level = "LOW"
            if event.get("fraud_score", 0) > 70:
                risk_level = "HIGH"
            elif event.get("fraud_score", 0) > 40:
                risk_level = "MEDIUM"
            
            result = {
                "transaction_id": event.get("transaction_id"),
                "account_id": event.get("account_id"),
                "amount": event.get("amount"),
                "risk_level": risk_level,
                "processed_at": datetime.utcnow().isoformat(),
            }
            results.append(result)
        
        return results
    
    def handle_fraud_alerts(self, messages: List[StreamMessage]) -> List[Dict[str, Any]]:
        """Process fraud alert events."""
        results = []
        
        for msg in messages:
            event = msg.value
            self.fraud_alerts += 1
            
            # In production, this would trigger notifications
            result = {
                "alert_id": event.get("alert_id"),
                "transaction_id": event.get("transaction_id"),
                "severity": event.get("severity"),
                "action": "BLOCK" if event.get("severity") == "HIGH" else "REVIEW",
                "processed_at": datetime.utcnow().isoformat(),
            }
            results.append(result)
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get handler statistics."""
        return {
            "transaction_count": self.transaction_count,
            "total_volume": round(self.total_volume, 2),
            "avg_transaction": round(self.total_volume / max(1, self.transaction_count), 2),
            "fraud_alerts": self.fraud_alerts,
        }


class TechEventHandler:
    """Handler for tech domain events."""
    
    def __init__(self):
        self.metric_count = 0
        self.incident_count = 0
        self.high_cpu_alerts = 0
        self.cpu_aggregator = WindowedAggregator(window_size_seconds=60)
        self.memory_aggregator = WindowedAggregator(window_size_seconds=60)
    
    def handle_metrics(self, messages: List[StreamMessage]) -> List[Dict[str, Any]]:
        """Process server metric events."""
        results = []
        
        for msg in messages:
            event = msg.value
            self.metric_count += 1
            
            server_id = event.get("server_id")
            cpu = event.get("cpu_usage_pct", 0)
            memory = event.get("memory_used_pct", 0)
            
            self.cpu_aggregator.add(server_id, cpu)
            self.memory_aggregator.add(server_id, memory)
            
            # Generate alerts for high resource usage
            alerts = []
            if cpu > 90:
                alerts.append("Critical CPU usage")
                self.high_cpu_alerts += 1
            elif cpu > 80:
                alerts.append("High CPU usage")
            
            if memory > 90:
                alerts.append("Critical memory usage")
            elif memory > 80:
                alerts.append("High memory usage")
            
            result = {
                "server_id": server_id,
                "cpu_usage": cpu,
                "memory_usage": memory,
                "alerts": alerts,
                "processed_at": datetime.utcnow().isoformat(),
            }
            results.append(result)
        
        return results
    
    def handle_incidents(self, messages: List[StreamMessage]) -> List[Dict[str, Any]]:
        """Process incident events."""
        results = []
        
        for msg in messages:
            event = msg.value
            self.incident_count += 1
            
            # Auto-assign based on severity
            auto_page = event.get("severity") in ["SEV1", "SEV2"]
            
            result = {
                "incident_id": event.get("incident_id"),
                "server_id": event.get("server_id"),
                "severity": event.get("severity"),
                "auto_page": auto_page,
                "processed_at": datetime.utcnow().isoformat(),
            }
            results.append(result)
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get handler statistics."""
        return {
            "metric_count": self.metric_count,
            "incident_count": self.incident_count,
            "high_cpu_alerts": self.high_cpu_alerts,
        }


class UniversityEventHandler:
    """Handler for university domain events."""
    
    def __init__(self):
        self.enrollment_count = 0
        self.grade_count = 0
        self.total_grade_points = 0.0
        self.total_credits = 0
    
    def handle_enrollments(self, messages: List[StreamMessage]) -> List[Dict[str, Any]]:
        """Process enrollment events."""
        results = []
        
        for msg in messages:
            event = msg.value
            self.enrollment_count += 1
            
            result = {
                "enrollment_id": event.get("enrollment_id"),
                "student_id": event.get("student_id"),
                "course_code": event.get("course_code"),
                "status": "CONFIRMED",
                "processed_at": datetime.utcnow().isoformat(),
            }
            results.append(result)
        
        return results
    
    def handle_grades(self, messages: List[StreamMessage]) -> List[Dict[str, Any]]:
        """Process grade events."""
        results = []
        
        for msg in messages:
            event = msg.value
            self.grade_count += 1
            
            credits = event.get("credits", 3)
            grade_points = event.get("grade_points", 0)
            
            self.total_grade_points += grade_points * credits
            self.total_credits += credits
            
            result = {
                "enrollment_id": event.get("enrollment_id"),
                "student_id": event.get("student_id"),
                "grade": event.get("grade"),
                "processed_at": datetime.utcnow().isoformat(),
            }
            results.append(result)
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get handler statistics."""
        avg_gpa = self.total_grade_points / max(1, self.total_credits)
        return {
            "enrollment_count": self.enrollment_count,
            "grade_count": self.grade_count,
            "average_gpa": round(avg_gpa, 2),
        }


def create_domain_consumer() -> EventConsumer:
    """Create a consumer with all domain handlers configured."""
    consumer = EventConsumer(consumer_group="enterprise_platform")
    
    # Health handlers
    health_handler = HealthEventHandler()
    consumer.subscribe("health.encounters", health_handler.handle_encounters)
    consumer.subscribe("health.lab_results", health_handler.handle_lab_results)
    
    # Finance handlers
    finance_handler = FinanceEventHandler()
    consumer.subscribe("finance.transactions", finance_handler.handle_transactions)
    consumer.subscribe("finance.fraud_alerts", finance_handler.handle_fraud_alerts)
    
    # Tech handlers
    tech_handler = TechEventHandler()
    consumer.subscribe("tech.metrics", tech_handler.handle_metrics)
    consumer.subscribe("tech.incidents", tech_handler.handle_incidents)
    
    # University handlers
    uni_handler = UniversityEventHandler()
    consumer.subscribe("university.enrollments", uni_handler.handle_enrollments)
    consumer.subscribe("university.grades", uni_handler.handle_grades)
    
    return consumer
