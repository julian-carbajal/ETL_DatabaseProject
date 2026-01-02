"""
Event Producer for Streaming Pipeline
======================================

Produces events to the message broker for real-time processing.
"""

import json
import random
import time
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
import logging
import uuid

from .stream_processor import StreamMessage, InMemoryMessageBroker, get_broker

logger = logging.getLogger(__name__)


class EventProducer:
    """
    Produces events to topics for stream processing.
    Simulates real-time data generation.
    """
    
    def __init__(
        self,
        broker: InMemoryMessageBroker = None,
        default_topic: str = "events",
    ):
        self.broker = broker or get_broker()
        self.default_topic = default_topic
        self.broker.create_topic(default_topic)
        
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.logger = logging.getLogger(f"{__name__}.EventProducer")
        
        self.events_produced = 0
    
    def produce(
        self,
        value: Dict[str, Any],
        topic: str = None,
        key: str = None,
        headers: Dict[str, str] = None,
    ) -> int:
        """Produce a single event."""
        topic = topic or self.default_topic
        
        message = StreamMessage(
            id=str(uuid.uuid4()),
            topic=topic,
            key=key,
            value=value,
            headers=headers or {},
        )
        
        offset = self.broker.produce(topic, message)
        self.events_produced += 1
        
        return offset
    
    def produce_batch(
        self,
        events: List[Dict[str, Any]],
        topic: str = None,
    ) -> List[int]:
        """Produce a batch of events."""
        offsets = []
        for event in events:
            offset = self.produce(event, topic)
            offsets.append(offset)
        return offsets
    
    def start_continuous(
        self,
        generator: Callable[[], Dict[str, Any]],
        topic: str = None,
        events_per_second: float = 10.0,
    ):
        """Start continuous event generation."""
        if self._running:
            self.logger.warning("Producer already running")
            return
        
        self._running = True
        self._thread = threading.Thread(
            target=self._continuous_loop,
            args=(generator, topic, events_per_second),
            daemon=True,
        )
        self._thread.start()
        self.logger.info(f"Started continuous producer: {events_per_second} events/sec")
    
    def stop(self):
        """Stop continuous production."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
        self.logger.info("Producer stopped")
    
    def _continuous_loop(
        self,
        generator: Callable,
        topic: str,
        events_per_second: float,
    ):
        """Continuous event generation loop."""
        interval = 1.0 / events_per_second
        
        while self._running:
            try:
                event = generator()
                self.produce(event, topic)
            except Exception as e:
                self.logger.error(f"Error generating event: {e}")
            
            time.sleep(interval)


class DomainEventProducer:
    """
    Produces domain-specific events for real-time processing.
    """
    
    def __init__(self, broker: InMemoryMessageBroker = None):
        self.broker = broker or get_broker()
        self.producer = EventProducer(broker)
        
        # Create domain topics
        for topic in [
            "health.encounters",
            "health.lab_results",
            "finance.transactions",
            "finance.fraud_alerts",
            "tech.metrics",
            "tech.incidents",
            "university.enrollments",
            "university.grades",
        ]:
            self.broker.create_topic(topic)
    
    def produce_health_encounter(self) -> Dict[str, Any]:
        """Generate a health encounter event."""
        event = {
            "event_type": "ENCOUNTER_CREATED",
            "timestamp": datetime.utcnow().isoformat(),
            "patient_id": f"PAT{random.randint(10000000, 99999999)}",
            "provider_id": f"PRV{random.randint(10000, 99999)}",
            "encounter_type": random.choice(["OUTPATIENT", "INPATIENT", "EMERGENCY", "TELEHEALTH"]),
            "chief_complaint": random.choice([
                "Chest pain", "Headache", "Back pain", "Fever", "Cough",
                "Abdominal pain", "Fatigue", "Dizziness", "Shortness of breath"
            ]),
            "vitals": {
                "blood_pressure_systolic": random.randint(90, 180),
                "blood_pressure_diastolic": random.randint(60, 110),
                "heart_rate": random.randint(50, 120),
                "temperature": round(random.uniform(97.0, 103.0), 1),
                "respiratory_rate": random.randint(12, 24),
            },
        }
        self.producer.produce(event, "health.encounters", key=event["patient_id"])
        return event
    
    def produce_lab_result(self) -> Dict[str, Any]:
        """Generate a lab result event."""
        test_codes = [
            ("GLU", "Glucose", "mg/dL", 70, 100),
            ("HBA1C", "Hemoglobin A1C", "%", 4.0, 5.6),
            ("CHOL", "Total Cholesterol", "mg/dL", 125, 200),
            ("WBC", "White Blood Cell Count", "K/uL", 4.5, 11.0),
            ("HGB", "Hemoglobin", "g/dL", 12.0, 17.5),
        ]
        
        test = random.choice(test_codes)
        code, name, unit, low, high = test
        value = round(random.uniform(low * 0.7, high * 1.3), 2)
        
        event = {
            "event_type": "LAB_RESULT_RECEIVED",
            "timestamp": datetime.utcnow().isoformat(),
            "patient_id": f"PAT{random.randint(10000000, 99999999)}",
            "test_code": code,
            "test_name": name,
            "value": value,
            "unit": unit,
            "reference_low": low,
            "reference_high": high,
            "abnormal_flag": value < low or value > high,
            "critical_flag": value < low * 0.5 or value > high * 1.5,
        }
        self.producer.produce(event, "health.lab_results", key=event["patient_id"])
        return event
    
    def produce_transaction(self) -> Dict[str, Any]:
        """Generate a financial transaction event."""
        txn_type = random.choice(["DEBIT", "CREDIT"])
        amount = round(random.uniform(5, 5000), 2)
        
        event = {
            "event_type": "TRANSACTION_CREATED",
            "timestamp": datetime.utcnow().isoformat(),
            "transaction_id": f"TXN{uuid.uuid4().hex[:12].upper()}",
            "account_id": f"ACC{random.randint(1000000000, 9999999999)}",
            "transaction_type": txn_type,
            "amount": amount if txn_type == "CREDIT" else -amount,
            "currency": "USD",
            "merchant": random.choice([
                "Amazon", "Walmart", "Starbucks", "Shell", "Netflix",
                "Uber", "Target", "Costco", "Apple", "Google"
            ]),
            "category": random.choice([
                "Shopping", "Food", "Gas", "Entertainment", "Utilities"
            ]),
            "fraud_score": random.randint(0, 100),
        }
        
        # Flag suspicious transactions
        if event["fraud_score"] > 80 or abs(event["amount"]) > 3000:
            self.produce_fraud_alert(event)
        
        self.producer.produce(event, "finance.transactions", key=event["account_id"])
        return event
    
    def produce_fraud_alert(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a fraud alert event."""
        event = {
            "event_type": "FRAUD_ALERT",
            "timestamp": datetime.utcnow().isoformat(),
            "alert_id": f"FRD{uuid.uuid4().hex[:8].upper()}",
            "transaction_id": transaction["transaction_id"],
            "account_id": transaction["account_id"],
            "amount": transaction["amount"],
            "fraud_score": transaction["fraud_score"],
            "alert_reason": random.choice([
                "High fraud score", "Unusual amount", "Suspicious merchant",
                "Geographic anomaly", "Velocity check failed"
            ]),
            "severity": "HIGH" if transaction["fraud_score"] > 90 else "MEDIUM",
        }
        self.producer.produce(event, "finance.fraud_alerts", key=event["account_id"])
        return event
    
    def produce_server_metric(self) -> Dict[str, Any]:
        """Generate a server metric event."""
        event = {
            "event_type": "METRIC_RECORDED",
            "timestamp": datetime.utcnow().isoformat(),
            "server_id": f"SRV-{random.choice(['US', 'EU', 'AP'])}-{random.randint(1000, 9999)}",
            "cpu_usage_pct": round(random.uniform(10, 90), 2),
            "memory_used_pct": round(random.uniform(30, 85), 2),
            "disk_used_pct": round(random.uniform(20, 80), 2),
            "network_in_mb_s": round(random.uniform(0, 100), 2),
            "network_out_mb_s": round(random.uniform(0, 50), 2),
            "request_count": random.randint(100, 10000),
            "error_count": random.randint(0, 50),
            "latency_p99_ms": random.randint(50, 500),
        }
        self.producer.produce(event, "tech.metrics", key=event["server_id"])
        return event
    
    def produce_incident(self) -> Dict[str, Any]:
        """Generate an incident event."""
        event = {
            "event_type": "INCIDENT_CREATED",
            "timestamp": datetime.utcnow().isoformat(),
            "incident_id": f"INC{datetime.utcnow().strftime('%Y%m%d')}{random.randint(100, 999)}",
            "server_id": f"SRV-{random.choice(['US', 'EU', 'AP'])}-{random.randint(1000, 9999)}",
            "severity": random.choice(["SEV1", "SEV2", "SEV3", "SEV4"]),
            "category": random.choice(["INFRASTRUCTURE", "APPLICATION", "NETWORK", "SECURITY"]),
            "title": random.choice([
                "High CPU Usage", "Memory Exhaustion", "Disk Full",
                "Service Unavailable", "High Latency", "Connection Timeout"
            ]),
            "status": "OPEN",
            "assigned_team": random.choice(["SRE", "DevOps", "Platform", "Security"]),
        }
        self.producer.produce(event, "tech.incidents", key=event["server_id"])
        return event
    
    def produce_enrollment(self) -> Dict[str, Any]:
        """Generate an enrollment event."""
        event = {
            "event_type": "ENROLLMENT_CREATED",
            "timestamp": datetime.utcnow().isoformat(),
            "enrollment_id": f"ENR{uuid.uuid4().hex[:8].upper()}",
            "student_id": f"STU{random.randint(10000000, 99999999)}",
            "course_id": f"CRS{random.randint(10000000, 99999999)}",
            "course_code": f"{random.choice(['CS', 'MATH', 'PHYS', 'ECON'])}{random.choice([101, 201, 301, 401])}",
            "term": f"{'FALL' if random.random() > 0.5 else 'SPRING'}_{random.randint(2024, 2025)}",
            "credits": random.choice([3, 4]),
        }
        self.producer.produce(event, "university.enrollments", key=event["student_id"])
        return event
    
    def produce_grade(self) -> Dict[str, Any]:
        """Generate a grade event."""
        grades = ["A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D", "F"]
        grade_points = [4.0, 3.7, 3.3, 3.0, 2.7, 2.3, 2.0, 1.7, 1.0, 0.0]
        idx = random.choices(range(len(grades)), weights=[15, 12, 15, 18, 12, 10, 8, 5, 3, 2])[0]
        
        event = {
            "event_type": "GRADE_POSTED",
            "timestamp": datetime.utcnow().isoformat(),
            "enrollment_id": f"ENR{uuid.uuid4().hex[:8].upper()}",
            "student_id": f"STU{random.randint(10000000, 99999999)}",
            "course_id": f"CRS{random.randint(10000000, 99999999)}",
            "grade": grades[idx],
            "grade_points": grade_points[idx],
            "credits": random.choice([3, 4]),
        }
        self.producer.produce(event, "university.grades", key=event["student_id"])
        return event
