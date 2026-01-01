#!/usr/bin/env python3
"""
Quick Demo Script - Enterprise Data Engineering Platform
=========================================================

A simplified demo that showcases the key capabilities without
requiring external dependencies beyond Python standard library.

Run: python demo.py
"""

import sys
import json
from pathlib import Path
from datetime import datetime

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from generators import (
    HealthDataGenerator, FinanceDataGenerator,
    TechDataGenerator, UniversityDataGenerator
)
from quality.validators import (
    DataValidator, create_health_validator, 
    create_finance_validator, create_university_validator
)
from quality.profiler import DataProfiler, DataQualityScorer
from analytics.aggregations import MetricsCalculator


def main():
    print("\n" + "="*70)
    print("  ENTERPRISE DATA ENGINEERING PLATFORM - QUICK DEMO")
    print("  Health • Finance • Tech • University")
    print("="*70 + "\n")
    
    # =========================================================================
    # 1. DATA GENERATION
    # =========================================================================
    print("📊 STEP 1: Generating Synthetic Data")
    print("-" * 50)
    
    # Health Data
    health_gen = HealthDataGenerator(seed=42)
    health_data = health_gen.generate_complete_dataset(
        num_patients=50, num_providers=10
    )
    print(f"  ✓ Health: {len(health_data['patients'])} patients, "
          f"{len(health_data['encounters'])} encounters")
    
    # Finance Data
    finance_gen = FinanceDataGenerator(seed=42)
    finance_data = finance_gen.generate_complete_dataset(
        num_accounts=50, transactions_per_account=20
    )
    print(f"  ✓ Finance: {len(finance_data['accounts'])} accounts, "
          f"{len(finance_data['transactions'])} transactions")
    
    # Tech Data
    tech_gen = TechDataGenerator(seed=42)
    tech_data = tech_gen.generate_complete_dataset(
        num_servers=25, apps_per_server=2
    )
    print(f"  ✓ Tech: {len(tech_data['servers'])} servers, "
          f"{len(tech_data['incidents'])} incidents")
    
    # University Data
    uni_gen = UniversityDataGenerator(seed=42)
    uni_data = uni_gen.generate_complete_dataset(
        num_students=100, num_faculty=20
    )
    print(f"  ✓ University: {len(uni_data['students'])} students, "
          f"{len(uni_data['courses'])} courses")
    
    # =========================================================================
    # 2. DATA QUALITY VALIDATION
    # =========================================================================
    print("\n🔍 STEP 2: Data Quality Validation")
    print("-" * 50)
    
    profiler = DataProfiler()
    scorer = DataQualityScorer()
    
    # Validate Health Data
    health_validator = create_health_validator()
    health_report = health_validator.validate(health_data['patients'], 'patient_id')
    health_profile = profiler.profile(health_data['patients'], 'patients')
    health_score = scorer.score(health_profile, health_report)
    print(f"  Health Data Quality:")
    print(f"    - Validity: {health_report.validity_rate:.1%}")
    print(f"    - Completeness: {health_profile.overall_completeness:.1%}")
    print(f"    - Score: {health_score['overall_score']:.0f}/100 ({health_score['grade']})")
    
    # Validate Finance Data
    finance_validator = create_finance_validator()
    finance_report = finance_validator.validate(finance_data['accounts'], 'account_number')
    finance_profile = profiler.profile(finance_data['accounts'], 'accounts')
    finance_score = scorer.score(finance_profile, finance_report)
    print(f"  Finance Data Quality:")
    print(f"    - Validity: {finance_report.validity_rate:.1%}")
    print(f"    - Completeness: {finance_profile.overall_completeness:.1%}")
    print(f"    - Score: {finance_score['overall_score']:.0f}/100 ({finance_score['grade']})")
    
    # Validate University Data
    uni_validator = create_university_validator()
    uni_report = uni_validator.validate(uni_data['students'], 'student_id')
    uni_profile = profiler.profile(uni_data['students'], 'students')
    uni_score = scorer.score(uni_profile, uni_report)
    print(f"  University Data Quality:")
    print(f"    - Validity: {uni_report.validity_rate:.1%}")
    print(f"    - Completeness: {uni_profile.overall_completeness:.1%}")
    print(f"    - Score: {uni_score['overall_score']:.0f}/100 ({uni_score['grade']})")
    
    # =========================================================================
    # 3. ANALYTICS & METRICS
    # =========================================================================
    print("\n📈 STEP 3: Analytics & Metrics")
    print("-" * 50)
    
    calculator = MetricsCalculator()
    
    # Health Metrics
    health_metrics = calculator.calculate_health_metrics(health_data)
    print(f"  Health Metrics:")
    print(f"    - Total Patients: {health_metrics.get('total_patients', 0)}")
    print(f"    - Avg Patient Age: {health_metrics.get('avg_patient_age', 'N/A')}")
    print(f"    - Abnormal Lab Rate: {health_metrics.get('abnormal_lab_rate', 0):.1%}")
    
    # Finance Metrics
    finance_metrics = calculator.calculate_finance_metrics(finance_data)
    print(f"  Finance Metrics:")
    print(f"    - Total Accounts: {finance_metrics.get('total_accounts', 0)}")
    print(f"    - Total Balance: ${finance_metrics.get('total_balance', 0):,.2f}")
    print(f"    - Flagged Txn Rate: {finance_metrics.get('flagged_transaction_rate', 0):.2%}")
    
    # Tech Metrics
    tech_metrics = calculator.calculate_tech_metrics(tech_data)
    print(f"  Tech Metrics:")
    print(f"    - Total Servers: {tech_metrics.get('total_servers', 0)}")
    print(f"    - Availability: {tech_metrics.get('server_availability_rate', 0):.1%}")
    print(f"    - MTTR: {tech_metrics.get('mttr_minutes', 0):.0f} minutes")
    
    # University Metrics
    uni_metrics = calculator.calculate_university_metrics(uni_data)
    print(f"  University Metrics:")
    print(f"    - Total Students: {uni_metrics.get('total_students', 0)}")
    print(f"    - Average GPA: {uni_metrics.get('avg_gpa', 0):.2f}")
    print(f"    - Retention Rate: {uni_metrics.get('retention_rate', 0):.1%}")
    
    # =========================================================================
    # 4. SAMPLE DATA OUTPUT
    # =========================================================================
    print("\n📋 STEP 4: Sample Records")
    print("-" * 50)
    
    # Sample patient
    patient = health_data['patients'][0]
    print(f"  Sample Patient:")
    print(f"    ID: {patient['patient_id']}")
    print(f"    Name: {patient['first_name']} {patient['last_name']}")
    print(f"    DOB: {patient['date_of_birth']}")
    print(f"    Blood Type: {patient['blood_type']}")
    
    # Sample account
    account = finance_data['accounts'][0]
    print(f"  Sample Account:")
    print(f"    Number: {account['account_number']}")
    print(f"    Type: {account['account_type']}")
    print(f"    Balance: ${account['current_balance']:,.2f}")
    
    # Sample server
    server = tech_data['servers'][0]
    print(f"  Sample Server:")
    print(f"    ID: {server['server_id']}")
    print(f"    Hostname: {server['hostname']}")
    print(f"    Environment: {server['environment']}")
    print(f"    Status: {server['status']}")
    
    # Sample student
    student = uni_data['students'][0]
    print(f"  Sample Student:")
    print(f"    ID: {student['student_id']}")
    print(f"    Name: {student['first_name']} {student['last_name']}")
    print(f"    Major: {student['major']}")
    print(f"    GPA: {student['gpa']}")
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "="*70)
    print("  DEMO COMPLETE!")
    print("="*70)
    print("""
  This demo showcased:
  
  ✓ Multi-domain synthetic data generation
    - Healthcare (HIPAA-style patient records)
    - Finance (SOX-compliant transactions)
    - Tech (Infrastructure monitoring)
    - University (Academic records)
  
  ✓ Data quality validation
    - Schema validation
    - Business rule checks
    - Quality scoring
  
  ✓ Analytics and metrics calculation
    - Domain-specific KPIs
    - Aggregations and summaries
  
  For the full experience, run: python main.py
  This will generate reports, run ETL pipelines, and more!
""")


if __name__ == "__main__":
    main()
