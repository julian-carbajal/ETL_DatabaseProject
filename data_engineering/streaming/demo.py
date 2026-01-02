#!/usr/bin/env python3
"""
Streaming Pipeline Demo
=======================

Demonstrates real-time event processing across all domains.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from streaming.stream_processor import StreamProcessor, StreamConfig, get_broker
from streaming.event_producer import DomainEventProducer
from streaming.event_consumer import (
    EventConsumer, HealthEventHandler, FinanceEventHandler,
    TechEventHandler, UniversityEventHandler
)


def main():
    print("\n" + "="*60)
    print("  STREAMING PIPELINE DEMO")
    print("  Real-time Event Processing")
    print("="*60 + "\n")
    
    # Initialize producer
    producer = DomainEventProducer()
    
    # Initialize consumer with handlers
    consumer = EventConsumer(consumer_group="demo")
    
    health_handler = HealthEventHandler()
    finance_handler = FinanceEventHandler()
    tech_handler = TechEventHandler()
    uni_handler = UniversityEventHandler()
    
    consumer.subscribe("health.encounters", health_handler.handle_encounters)
    consumer.subscribe("health.lab_results", health_handler.handle_lab_results)
    consumer.subscribe("finance.transactions", finance_handler.handle_transactions)
    consumer.subscribe("finance.fraud_alerts", finance_handler.handle_fraud_alerts)
    consumer.subscribe("tech.metrics", tech_handler.handle_metrics)
    consumer.subscribe("tech.incidents", tech_handler.handle_incidents)
    consumer.subscribe("university.enrollments", uni_handler.handle_enrollments)
    consumer.subscribe("university.grades", uni_handler.handle_grades)
    
    # Start consumer
    consumer.start_all()
    
    print("📡 Producing events...")
    print("-" * 40)
    
    # Produce events
    events_per_domain = 20
    
    print(f"\n🏥 Health Domain ({events_per_domain * 2} events)")
    for _ in range(events_per_domain):
        producer.produce_health_encounter()
        producer.produce_lab_result()
    
    print(f"💰 Finance Domain ({events_per_domain} transactions)")
    for _ in range(events_per_domain):
        producer.produce_transaction()
    
    print(f"🖥️  Tech Domain ({events_per_domain * 2} events)")
    for _ in range(events_per_domain):
        producer.produce_server_metric()
        if _ % 5 == 0:
            producer.produce_incident()
    
    print(f"🎓 University Domain ({events_per_domain * 2} events)")
    for _ in range(events_per_domain):
        producer.produce_enrollment()
        producer.produce_grade()
    
    # Wait for processing
    print("\n⏳ Processing events...")
    time.sleep(1)
    
    # Stop consumer
    consumer.stop_all()
    
    # Print results
    print("\n" + "="*60)
    print("  PROCESSING RESULTS")
    print("="*60)
    
    print("\n🏥 Health Stats:")
    stats = health_handler.get_stats()
    print(f"   Encounters: {stats['encounter_count']}")
    print(f"   Abnormal Labs: {stats['abnormal_labs']}")
    print(f"   Critical Labs: {stats['critical_labs']}")
    
    print("\n💰 Finance Stats:")
    stats = finance_handler.get_stats()
    print(f"   Transactions: {stats['transaction_count']}")
    print(f"   Total Volume: ${stats['total_volume']:,.2f}")
    print(f"   Fraud Alerts: {stats['fraud_alerts']}")
    
    print("\n🖥️  Tech Stats:")
    stats = tech_handler.get_stats()
    print(f"   Metrics: {stats['metric_count']}")
    print(f"   Incidents: {stats['incident_count']}")
    print(f"   High CPU Alerts: {stats['high_cpu_alerts']}")
    
    print("\n🎓 University Stats:")
    stats = uni_handler.get_stats()
    print(f"   Enrollments: {stats['enrollment_count']}")
    print(f"   Grades Posted: {stats['grade_count']}")
    print(f"   Average GPA: {stats['average_gpa']}")
    
    print("\n✅ Streaming demo complete!\n")


if __name__ == "__main__":
    main()
