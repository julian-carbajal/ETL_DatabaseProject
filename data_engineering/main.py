#!/usr/bin/env python3
"""
Enterprise Data Engineering Platform - Main Runner
===================================================

A comprehensive data engineering project spanning Health, Tech, Finance, and University domains.
Demonstrates real-world data engineering skills including:
- ETL Pipeline Development
- Data Quality Management
- Data Modeling & Schema Design
- Analytics & Reporting
- Monitoring & Alerting

Author: Julian Carbajal
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# Import project modules
from config import (
    DB_CONFIG, ETL_CONFIG, DATA_DIR, RAW_DATA_DIR, 
    PROCESSED_DATA_DIR, WAREHOUSE_DIR, LOGS_DIR
)
from generators import (
    HealthDataGenerator, FinanceDataGenerator,
    TechDataGenerator, UniversityDataGenerator
)
from etl.base import ETLPipeline, ETLContext
from etl.extractors import CSVExtractor, JSONExtractor
from etl.transformers import (
    CleansingTransformer, EnrichmentTransformer,
    ValidationTransformer, DeduplicationTransformer,
    PIIMaskingTransformer, TypeCastTransformer
)
from etl.loaders import DatabaseLoader, FileLoader
from etl.orchestrator import PipelineOrchestrator, WorkflowBuilder
from quality.validators import (
    DataValidator, create_health_validator, create_finance_validator,
    create_tech_validator, create_university_validator
)
from quality.profiler import DataProfiler, DataQualityScorer
from quality.monitors import DataQualityMonitor, AlertManager, console_alert_handler, AlertSeverity
from analytics.aggregations import MetricsCalculator
from analytics.reports import ReportGenerator


# ANSI Colors for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


def print_banner():
    """Print application banner."""
    banner = f"""
{Colors.CYAN}{Colors.BOLD}
╔═══════════════════════════════════════════════════════════════════════════════╗
║                                                                               ║
║   ███████╗███╗   ██╗████████╗███████╗██████╗ ██████╗ ██████╗ ██╗███████╗███████╗║
║   ██╔════╝████╗  ██║╚══██╔══╝██╔════╝██╔══██╗██╔══██╗██╔══██╗██║██╔════╝██╔════╝║
║   █████╗  ██╔██╗ ██║   ██║   █████╗  ██████╔╝██████╔╝██████╔╝██║███████╗█████╗  ║
║   ██╔══╝  ██║╚██╗██║   ██║   ██╔══╝  ██╔══██╗██╔═══╝ ██╔══██╗██║╚════██║██╔══╝  ║
║   ███████╗██║ ╚████║   ██║   ███████╗██║  ██║██║     ██║  ██║██║███████║███████╗║
║   ╚══════╝╚═╝  ╚═══╝   ╚═╝   ╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝╚═╝╚══════╝╚══════╝║
║                                                                               ║
║              DATA ENGINEERING PLATFORM v1.0                                   ║
║              Health • Finance • Tech • University                             ║
║                                                                               ║
╚═══════════════════════════════════════════════════════════════════════════════╝
{Colors.END}
"""
    print(banner)


def print_section(title: str):
    """Print section header."""
    print(f"\n{Colors.BLUE}{Colors.BOLD}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}{Colors.BOLD}  {title}{Colors.END}")
    print(f"{Colors.BLUE}{Colors.BOLD}{'='*60}{Colors.END}\n")


def generate_all_data(seed: int = 42) -> dict:
    """Generate synthetic data for all domains."""
    print_section("GENERATING SYNTHETIC DATA")
    
    all_data = {}
    
    # Health Domain
    print(f"{Colors.GREEN}► Generating Health Data...{Colors.END}")
    health_gen = HealthDataGenerator(seed=seed)
    all_data["health"] = health_gen.generate_complete_dataset(
        num_patients=200,
        num_providers=30,
        diagnoses_per_patient=3.0,
        medications_per_patient=2.5,
        labs_per_patient=5.0,
        encounters_per_patient=4.0
    )
    print(f"  ✓ Patients: {len(all_data['health']['patients'])}")
    print(f"  ✓ Providers: {len(all_data['health']['providers'])}")
    print(f"  ✓ Diagnoses: {len(all_data['health']['diagnoses'])}")
    print(f"  ✓ Medications: {len(all_data['health']['medications'])}")
    print(f"  ✓ Lab Results: {len(all_data['health']['lab_results'])}")
    print(f"  ✓ Encounters: {len(all_data['health']['encounters'])}")
    
    # Finance Domain
    print(f"\n{Colors.GREEN}► Generating Finance Data...{Colors.END}")
    finance_gen = FinanceDataGenerator(seed=seed)
    all_data["finance"] = finance_gen.generate_complete_dataset(
        num_accounts=150,
        transactions_per_account=50.0,
        investments_per_account=5.0,
        num_loan_applications=75,
        credit_scores_per_customer=3.0,
        num_financial_statements=100
    )
    print(f"  ✓ Accounts: {len(all_data['finance']['accounts'])}")
    print(f"  ✓ Transactions: {len(all_data['finance']['transactions'])}")
    print(f"  ✓ Investments: {len(all_data['finance']['investments'])}")
    print(f"  ✓ Loan Applications: {len(all_data['finance']['loan_applications'])}")
    print(f"  ✓ Credit Scores: {len(all_data['finance']['credit_scores'])}")
    print(f"  ✓ Financial Statements: {len(all_data['finance']['financial_statements'])}")
    
    # Tech Domain
    print(f"\n{Colors.GREEN}► Generating Tech Data...{Colors.END}")
    tech_gen = TechDataGenerator(seed=seed)
    all_data["tech"] = tech_gen.generate_complete_dataset(
        num_servers=75,
        apps_per_server=2.0,
        incidents_per_month=25,
        deployments_per_app=5.0,
        metrics_per_server=100,
        endpoints_per_app=8.0
    )
    print(f"  ✓ Servers: {len(all_data['tech']['servers'])}")
    print(f"  ✓ Applications: {len(all_data['tech']['applications'])}")
    print(f"  ✓ Incidents: {len(all_data['tech']['incidents'])}")
    print(f"  ✓ Deployments: {len(all_data['tech']['deployments'])}")
    print(f"  ✓ Metrics: {len(all_data['tech']['metrics'])}")
    print(f"  ✓ API Endpoints: {len(all_data['tech']['api_endpoints'])}")
    
    # University Domain
    print(f"\n{Colors.GREEN}► Generating University Data...{Colors.END}")
    uni_gen = UniversityDataGenerator(seed=seed)
    all_data["university"] = uni_gen.generate_complete_dataset(
        num_students=500,
        num_faculty=100,
        enrollments_per_student=8.0,
        grants_per_faculty=0.5
    )
    print(f"  ✓ Departments: {len(all_data['university']['departments'])}")
    print(f"  ✓ Faculty: {len(all_data['university']['faculty'])}")
    print(f"  ✓ Students: {len(all_data['university']['students'])}")
    print(f"  ✓ Courses: {len(all_data['university']['courses'])}")
    print(f"  ✓ Enrollments: {len(all_data['university']['enrollments'])}")
    print(f"  ✓ Research Grants: {len(all_data['university']['research_grants'])}")
    
    return all_data


def save_raw_data(all_data: dict):
    """Save generated data to raw data directory."""
    print_section("SAVING RAW DATA")
    
    for domain, domain_data in all_data.items():
        domain_dir = RAW_DATA_DIR / domain
        domain_dir.mkdir(parents=True, exist_ok=True)
        
        for entity, records in domain_data.items():
            file_path = domain_dir / f"{entity}.json"
            with open(file_path, "w") as f:
                json.dump(records, f, indent=2, default=str)
            print(f"  ✓ Saved {domain}/{entity}.json ({len(records)} records)")


def run_data_quality_checks(all_data: dict) -> dict:
    """Run data quality validation and profiling."""
    print_section("DATA QUALITY CHECKS")
    
    profiler = DataProfiler()
    scorer = DataQualityScorer()
    results = {}
    
    validators = {
        "health": ("patients", create_health_validator()),
        "finance": ("accounts", create_finance_validator()),
        "tech": ("servers", create_tech_validator()),
        "university": ("students", create_university_validator()),
    }
    
    for domain, (entity, validator) in validators.items():
        print(f"\n{Colors.CYAN}► {domain.upper()} Domain{Colors.END}")
        
        data = all_data[domain][entity]
        
        # Profile data
        profile = profiler.profile(data, f"{domain}_{entity}")
        
        # Validate data
        validation_report = validator.validate(data)
        
        # Calculate quality score
        quality_score = scorer.score(profile, validation_report)
        
        results[domain] = {
            "profile": profile.to_dict(),
            "validation": validation_report.to_dict(),
            "quality_score": quality_score,
        }
        
        print(f"  Records: {profile.row_count}")
        print(f"  Completeness: {profile.overall_completeness:.1%}")
        print(f"  Validity Rate: {validation_report.validity_rate:.1%}")
        print(f"  Quality Score: {quality_score['overall_score']:.1f}/100 ({quality_score['grade']})")
        
        if validation_report.error_count > 0:
            print(f"  {Colors.YELLOW}⚠ Validation Errors: {validation_report.error_count}{Colors.END}")
    
    return results


def run_data_monitoring(all_data: dict):
    """Run data quality monitoring with alerts."""
    print_section("DATA QUALITY MONITORING")
    
    # Setup alert manager
    alert_manager = AlertManager()
    alert_manager.add_handler(console_alert_handler, [AlertSeverity.CRITICAL, AlertSeverity.HIGH])
    
    # Setup monitors for each domain
    monitors = {
        "health": DataQualityMonitor("HealthMonitor"),
        "finance": DataQualityMonitor("FinanceMonitor"),
        "tech": DataQualityMonitor("TechMonitor"),
        "university": DataQualityMonitor("UniversityMonitor"),
    }
    
    # Configure monitoring rules
    monitors["health"].add_completeness_rule("first_name", 0.99)
    monitors["health"].add_completeness_rule("last_name", 0.99)
    monitors["health"].add_row_count_rule(min_rows=100)
    
    monitors["finance"].add_completeness_rule("account_number", 1.0)
    monitors["finance"].add_uniqueness_rule("account_number", 1.0)
    monitors["finance"].add_row_count_rule(min_rows=50)
    
    monitors["tech"].add_completeness_rule("server_id", 1.0)
    monitors["tech"].add_completeness_rule("hostname", 1.0)
    
    monitors["university"].add_completeness_rule("student_id", 1.0)
    monitors["university"].add_completeness_rule("email", 0.99)
    
    # Run monitoring
    entity_map = {
        "health": "patients",
        "finance": "accounts",
        "tech": "servers",
        "university": "students",
    }
    
    total_alerts = 0
    for domain, monitor in monitors.items():
        entity = entity_map[domain]
        data = all_data[domain][entity]
        
        result = monitor.monitor(data, f"{domain}_{entity}")
        alerts = result.get("alerts", [])
        total_alerts += len(alerts)
        
        print(f"  {domain.upper()}: {len(alerts)} alerts")
    
    print(f"\n  Total Alerts: {total_alerts}")


def calculate_analytics(all_data: dict) -> dict:
    """Calculate domain-specific analytics and metrics."""
    print_section("ANALYTICS & METRICS")
    
    calculator = MetricsCalculator()
    all_metrics = {}
    
    # Health Metrics
    print(f"{Colors.CYAN}► Health Domain Metrics{Colors.END}")
    health_metrics = calculator.calculate_health_metrics(all_data["health"])
    all_metrics["health"] = health_metrics
    print(f"  Total Patients: {health_metrics.get('total_patients', 0)}")
    print(f"  Avg Patient Age: {health_metrics.get('avg_patient_age', 'N/A')}")
    print(f"  Abnormal Lab Rate: {health_metrics.get('abnormal_lab_rate', 0):.1%}")
    
    # Finance Metrics
    print(f"\n{Colors.CYAN}► Finance Domain Metrics{Colors.END}")
    finance_metrics = calculator.calculate_finance_metrics(all_data["finance"])
    all_metrics["finance"] = finance_metrics
    print(f"  Total Accounts: {finance_metrics.get('total_accounts', 0)}")
    print(f"  Total Balance: ${finance_metrics.get('total_balance', 0):,.2f}")
    print(f"  Loan Approval Rate: {finance_metrics.get('loan_approval_rate', 0):.1%}")
    
    # Tech Metrics
    print(f"\n{Colors.CYAN}► Tech Domain Metrics{Colors.END}")
    tech_metrics = calculator.calculate_tech_metrics(all_data["tech"])
    all_metrics["tech"] = tech_metrics
    print(f"  Total Servers: {tech_metrics.get('total_servers', 0)}")
    print(f"  Server Availability: {tech_metrics.get('server_availability_rate', 0):.1%}")
    print(f"  MTTR: {tech_metrics.get('mttr_minutes', 0):.0f} minutes")
    print(f"  Deployment Success Rate: {tech_metrics.get('deployment_success_rate', 0):.1%}")
    
    # University Metrics
    print(f"\n{Colors.CYAN}► University Domain Metrics{Colors.END}")
    uni_metrics = calculator.calculate_university_metrics(all_data["university"])
    all_metrics["university"] = uni_metrics
    print(f"  Total Students: {uni_metrics.get('total_students', 0)}")
    print(f"  Total Faculty: {uni_metrics.get('total_faculty', 0)}")
    print(f"  Average GPA: {uni_metrics.get('avg_gpa', 'N/A')}")
    print(f"  Retention Rate: {uni_metrics.get('retention_rate', 0):.1%}")
    
    return all_metrics


def generate_reports(all_data: dict, all_metrics: dict):
    """Generate analytics reports for all domains."""
    print_section("GENERATING REPORTS")
    
    report_gen = ReportGenerator()
    reports_dir = DATA_DIR / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    # Health Report
    health_report = report_gen.generate_health_report(
        all_data["health"], all_metrics["health"]
    )
    health_report.save(reports_dir / "health_report.html", "html")
    health_report.save(reports_dir / "health_report.md", "markdown")
    print(f"  ✓ Health Report saved")
    
    # Finance Report
    finance_report = report_gen.generate_finance_report(
        all_data["finance"], all_metrics["finance"]
    )
    finance_report.save(reports_dir / "finance_report.html", "html")
    finance_report.save(reports_dir / "finance_report.md", "markdown")
    print(f"  ✓ Finance Report saved")
    
    # Tech Report
    tech_report = report_gen.generate_tech_report(
        all_data["tech"], all_metrics["tech"]
    )
    tech_report.save(reports_dir / "tech_report.html", "html")
    tech_report.save(reports_dir / "tech_report.md", "markdown")
    print(f"  ✓ Tech Report saved")
    
    # University Report
    uni_report = report_gen.generate_university_report(
        all_data["university"], all_metrics["university"]
    )
    uni_report.save(reports_dir / "university_report.html", "html")
    uni_report.save(reports_dir / "university_report.md", "markdown")
    print(f"  ✓ University Report saved")
    
    print(f"\n  Reports saved to: {reports_dir}")


def run_etl_demo(all_data: dict):
    """Demonstrate ETL pipeline capabilities."""
    print_section("ETL PIPELINE DEMONSTRATION")
    
    # Create a sample ETL pipeline for health data
    print(f"{Colors.CYAN}► Building Health Data Pipeline{Colors.END}")
    
    # Save sample data for extraction
    sample_file = RAW_DATA_DIR / "health" / "patients_sample.json"
    sample_file.parent.mkdir(parents=True, exist_ok=True)
    with open(sample_file, "w") as f:
        json.dump(all_data["health"]["patients"][:50], f, default=str)
    
    # Build pipeline
    pipeline = ETLPipeline(
        name="HealthPatientPipeline",
        description="Process patient data with cleansing and validation",
        max_retries=2
    )
    
    # Add transformers
    cleansing = CleansingTransformer(
        name="PatientCleansing",
        trim_strings=True,
        lowercase_columns=["email"],
        uppercase_columns=["state"],
    )
    
    enrichment = EnrichmentTransformer(
        name="PatientEnrichment",
        default_values={"country": "USA"},
        computed_fields={
            "full_name": lambda r: f"{r.get('first_name', '')} {r.get('last_name', '')}",
        }
    )
    
    dedup = DeduplicationTransformer(
        name="PatientDedup",
        key_columns=["patient_id"],
        keep="first"
    )
    
    pii_mask = PIIMaskingTransformer(
        name="PIIMasking",
        mask_columns={"email": "email", "phone": "phone"},
        hash_columns=["ssn_hash"]
    )
    
    # Add steps to pipeline
    pipeline.add_step(cleansing)
    pipeline.add_step(enrichment)
    pipeline.add_step(dedup)
    pipeline.add_step(pii_mask)
    
    # Create context with data
    context = ETLContext(job_name="health_patient_etl")
    context.raw_data = all_data["health"]["patients"][:50]
    
    # Run pipeline
    print(f"  Running pipeline with {len(context.raw_data)} records...")
    
    # Execute each step manually for demo
    for step in pipeline.steps:
        print(f"  → Executing: {step.name}")
        context = step.execute(context)
    
    print(f"\n  {Colors.GREEN}Pipeline completed successfully!{Colors.END}")
    print(f"  Records processed: {context.metrics.records_transformed}")
    print(f"  Duplicates removed: {context.metrics.records_duplicates}")
    print(f"  Total time: {context.metrics.total_time_seconds:.2f}s")


def print_summary():
    """Print final summary."""
    print_section("EXECUTION SUMMARY")
    
    print(f"""
{Colors.GREEN}✓ Data Generation Complete{Colors.END}
  - Health: Patients, Providers, Diagnoses, Medications, Labs, Encounters
  - Finance: Accounts, Transactions, Investments, Loans, Credit Scores
  - Tech: Servers, Applications, Incidents, Deployments, Metrics
  - University: Students, Faculty, Courses, Enrollments, Grants

{Colors.GREEN}✓ Data Quality Checks Complete{Colors.END}
  - Data profiling for all domains
  - Validation against domain-specific rules
  - Quality scoring with grades

{Colors.GREEN}✓ Analytics Calculated{Colors.END}
  - Domain-specific KPIs and metrics
  - Aggregations and summaries

{Colors.GREEN}✓ Reports Generated{Colors.END}
  - HTML and Markdown reports for each domain
  - Located in: data_engineering/data/reports/

{Colors.GREEN}✓ ETL Pipeline Demonstrated{Colors.END}
  - Cleansing, Enrichment, Deduplication, PII Masking

{Colors.CYAN}Project Structure:{Colors.END}
  data_engineering/
  ├── config/          # Configuration settings
  ├── models/          # SQLAlchemy data models
  ├── generators/      # Synthetic data generators
  ├── etl/             # ETL pipeline framework
  ├── quality/         # Data quality framework
  ├── analytics/       # Analytics and reporting
  └── data/            # Generated data and reports
""")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Enterprise Data Engineering Platform"
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for data generation (default: 42)"
    )
    parser.add_argument(
        "--skip-reports", action="store_true",
        help="Skip report generation"
    )
    parser.add_argument(
        "--skip-etl", action="store_true",
        help="Skip ETL demonstration"
    )
    
    args = parser.parse_args()
    
    print_banner()
    
    start_time = datetime.now()
    
    try:
        # Generate data
        all_data = generate_all_data(seed=args.seed)
        
        # Save raw data
        save_raw_data(all_data)
        
        # Run data quality checks
        quality_results = run_data_quality_checks(all_data)
        
        # Run monitoring
        run_data_monitoring(all_data)
        
        # Calculate analytics
        all_metrics = calculate_analytics(all_data)
        
        # Generate reports
        if not args.skip_reports:
            generate_reports(all_data, all_metrics)
        
        # Run ETL demo
        if not args.skip_etl:
            run_etl_demo(all_data)
        
        # Print summary
        print_summary()
        
        elapsed = datetime.now() - start_time
        print(f"\n{Colors.GREEN}Total execution time: {elapsed.total_seconds():.2f} seconds{Colors.END}\n")
        
    except Exception as e:
        logger.error(f"Execution failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
