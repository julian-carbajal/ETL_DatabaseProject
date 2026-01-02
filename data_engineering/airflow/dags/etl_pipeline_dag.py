"""
Apache Airflow DAG for ETL Pipeline Orchestration
==================================================

This DAG orchestrates the complete ETL pipeline across all domains.
Demonstrates real-world Airflow patterns for data engineering.

To use with Airflow:
1. Copy this file to your Airflow dags folder
2. Update the DATA_ENGINEERING_PATH variable
3. Enable the DAG in Airflow UI
"""

from datetime import datetime, timedelta
from typing import Dict, Any
import json
import sys
from pathlib import Path

# Airflow imports (will work when running in Airflow)
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator, BranchPythonOperator
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.bash import BashOperator
    from airflow.utils.task_group import TaskGroup
    from airflow.models import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    # Mock classes for development/testing
    class DAG:
        def __init__(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
    class PythonOperator:
        def __init__(self, *args, **kwargs): pass
    class BranchPythonOperator:
        def __init__(self, *args, **kwargs): pass
    class EmptyOperator:
        def __init__(self, *args, **kwargs): pass
    class BashOperator:
        def __init__(self, *args, **kwargs): pass
    class TaskGroup:
        def __init__(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass

# Path to data engineering project
DATA_ENGINEERING_PATH = Path(__file__).parent.parent.parent

# Default DAG arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}


# =============================================================================
# Task Functions
# =============================================================================

def generate_health_data(**context):
    """Generate synthetic health data."""
    sys.path.insert(0, str(DATA_ENGINEERING_PATH))
    from generators import HealthDataGenerator
    
    generator = HealthDataGenerator(seed=context.get('ds_nodash', 42))
    data = generator.generate_complete_dataset(
        num_patients=200,
        num_providers=30,
    )
    
    # Save to XCom for downstream tasks
    context['ti'].xcom_push(key='health_record_count', value=len(data['patients']))
    
    # Save to file
    output_dir = DATA_ENGINEERING_PATH / "data" / "raw" / "health"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for entity, records in data.items():
        with open(output_dir / f"{entity}.json", "w") as f:
            json.dump(records, f, default=str)
    
    return f"Generated {len(data['patients'])} patients"


def generate_finance_data(**context):
    """Generate synthetic finance data."""
    sys.path.insert(0, str(DATA_ENGINEERING_PATH))
    from generators import FinanceDataGenerator
    
    generator = FinanceDataGenerator(seed=context.get('ds_nodash', 42))
    data = generator.generate_complete_dataset(
        num_accounts=150,
        transactions_per_account=50,
    )
    
    context['ti'].xcom_push(key='finance_record_count', value=len(data['accounts']))
    
    output_dir = DATA_ENGINEERING_PATH / "data" / "raw" / "finance"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for entity, records in data.items():
        with open(output_dir / f"{entity}.json", "w") as f:
            json.dump(records, f, default=str)
    
    return f"Generated {len(data['accounts'])} accounts"


def generate_tech_data(**context):
    """Generate synthetic tech data."""
    sys.path.insert(0, str(DATA_ENGINEERING_PATH))
    from generators import TechDataGenerator
    
    generator = TechDataGenerator(seed=context.get('ds_nodash', 42))
    data = generator.generate_complete_dataset(
        num_servers=75,
        apps_per_server=2,
    )
    
    context['ti'].xcom_push(key='tech_record_count', value=len(data['servers']))
    
    output_dir = DATA_ENGINEERING_PATH / "data" / "raw" / "tech"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for entity, records in data.items():
        with open(output_dir / f"{entity}.json", "w") as f:
            json.dump(records, f, default=str)
    
    return f"Generated {len(data['servers'])} servers"


def generate_university_data(**context):
    """Generate synthetic university data."""
    sys.path.insert(0, str(DATA_ENGINEERING_PATH))
    from generators import UniversityDataGenerator
    
    generator = UniversityDataGenerator(seed=context.get('ds_nodash', 42))
    data = generator.generate_complete_dataset(
        num_students=500,
        num_faculty=100,
    )
    
    context['ti'].xcom_push(key='university_record_count', value=len(data['students']))
    
    output_dir = DATA_ENGINEERING_PATH / "data" / "raw" / "university"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    for entity, records in data.items():
        with open(output_dir / f"{entity}.json", "w") as f:
            json.dump(records, f, default=str)
    
    return f"Generated {len(data['students'])} students"


def validate_data(domain: str, **context):
    """Validate data quality for a domain."""
    sys.path.insert(0, str(DATA_ENGINEERING_PATH))
    from quality.validators import (
        create_health_validator, create_finance_validator,
        create_tech_validator, create_university_validator
    )
    from quality.profiler import DataProfiler, DataQualityScorer
    
    validators = {
        "health": ("patients", create_health_validator()),
        "finance": ("accounts", create_finance_validator()),
        "tech": ("servers", create_tech_validator()),
        "university": ("students", create_university_validator()),
    }
    
    entity, validator = validators[domain]
    
    # Load data
    data_file = DATA_ENGINEERING_PATH / "data" / "raw" / domain / f"{entity}.json"
    with open(data_file) as f:
        data = json.load(f)
    
    # Validate
    profiler = DataProfiler()
    scorer = DataQualityScorer()
    
    profile = profiler.profile(data, f"{domain}_{entity}")
    report = validator.validate(data)
    score = scorer.score(profile, report)
    
    # Push results to XCom
    context['ti'].xcom_push(key=f'{domain}_quality_score', value=score['overall_score'])
    context['ti'].xcom_push(key=f'{domain}_validity_rate', value=report.validity_rate)
    
    return {
        "domain": domain,
        "records": len(data),
        "validity_rate": report.validity_rate,
        "quality_score": score['overall_score'],
        "grade": score['grade'],
    }


def transform_to_warehouse(domain: str, **context):
    """Transform raw data to warehouse format."""
    sys.path.insert(0, str(DATA_ENGINEERING_PATH))
    from etl.transformers import CleansingTransformer, EnrichmentTransformer
    from etl.base import ETLContext
    
    # Load raw data
    raw_dir = DATA_ENGINEERING_PATH / "data" / "raw" / domain
    warehouse_dir = DATA_ENGINEERING_PATH / "data" / "warehouse" / domain
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    
    for json_file in raw_dir.glob("*.json"):
        with open(json_file) as f:
            data = json.load(f)
        
        # Apply transformations
        cleaner = CleansingTransformer(trim_strings=True)
        enricher = EnrichmentTransformer(
            default_values={"etl_load_date": datetime.now().isoformat()}
        )
        
        ctx = ETLContext()
        ctx.raw_data = data
        ctx = cleaner.execute(ctx)
        ctx.raw_data = ctx.transformed_data
        ctx = enricher.execute(ctx)
        
        # Save to warehouse
        with open(warehouse_dir / json_file.name, "w") as f:
            json.dump(ctx.transformed_data, f, default=str)
    
    return f"Transformed {domain} data to warehouse"


def generate_reports(**context):
    """Generate analytics reports."""
    sys.path.insert(0, str(DATA_ENGINEERING_PATH))
    from analytics.aggregations import MetricsCalculator
    from analytics.reports import ReportGenerator
    
    # Load warehouse data
    warehouse_dir = DATA_ENGINEERING_PATH / "data" / "warehouse"
    reports_dir = DATA_ENGINEERING_PATH / "data" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    all_data = {}
    for domain in ["health", "finance", "tech", "university"]:
        domain_dir = warehouse_dir / domain
        if domain_dir.exists():
            all_data[domain] = {}
            for json_file in domain_dir.glob("*.json"):
                with open(json_file) as f:
                    all_data[domain][json_file.stem] = json.load(f)
    
    # Calculate metrics
    calculator = MetricsCalculator()
    report_gen = ReportGenerator()
    
    metrics = {
        "health": calculator.calculate_health_metrics(all_data.get("health", {})),
        "finance": calculator.calculate_finance_metrics(all_data.get("finance", {})),
        "tech": calculator.calculate_tech_metrics(all_data.get("tech", {})),
        "university": calculator.calculate_university_metrics(all_data.get("university", {})),
    }
    
    # Generate reports
    for domain in ["health", "finance", "tech", "university"]:
        if domain in all_data:
            report_method = getattr(report_gen, f"generate_{domain}_report")
            report = report_method(all_data[domain], metrics[domain])
            report.save(reports_dir / f"{domain}_report.html", "html")
            report.save(reports_dir / f"{domain}_report.md", "markdown")
    
    return "Reports generated successfully"


def check_data_quality(**context):
    """Branch based on data quality scores."""
    ti = context['ti']
    
    scores = []
    for domain in ["health", "finance", "tech", "university"]:
        score = ti.xcom_pull(key=f'{domain}_quality_score', task_ids=f'validate_{domain}')
        if score:
            scores.append(score)
    
    avg_score = sum(scores) / len(scores) if scores else 0
    
    if avg_score >= 80:
        return 'transform_group'
    else:
        return 'quality_alert'


def send_quality_alert(**context):
    """Send alert for low data quality."""
    ti = context['ti']
    
    message = "Data Quality Alert!\n\n"
    for domain in ["health", "finance", "tech", "university"]:
        score = ti.xcom_pull(key=f'{domain}_quality_score', task_ids=f'validate_{domain}')
        validity = ti.xcom_pull(key=f'{domain}_validity_rate', task_ids=f'validate_{domain}')
        message += f"{domain.title()}: Score={score}, Validity={validity:.1%}\n"
    
    print(message)
    # In production, send to Slack/Email/PagerDuty
    return message


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id='enterprise_data_pipeline',
    default_args=default_args,
    description='Enterprise Data Engineering ETL Pipeline',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['data-engineering', 'etl', 'multi-domain'],
    doc_md="""
    # Enterprise Data Engineering Pipeline
    
    This DAG orchestrates the complete ETL pipeline:
    1. **Extract**: Generate synthetic data for all domains
    2. **Validate**: Run data quality checks
    3. **Transform**: Clean and enrich data
    4. **Load**: Store in data warehouse
    5. **Report**: Generate analytics reports
    
    ## Domains
    - Health (HIPAA compliant)
    - Finance (SOX compliant)
    - Tech (Infrastructure)
    - University (Academic)
    """,
) as dag:
    
    # Start
    start = EmptyOperator(task_id='start')
    
    # Extract: Generate data for all domains in parallel
    with TaskGroup(group_id='extract_group') as extract_group:
        extract_health = PythonOperator(
            task_id='extract_health',
            python_callable=generate_health_data,
        )
        extract_finance = PythonOperator(
            task_id='extract_finance',
            python_callable=generate_finance_data,
        )
        extract_tech = PythonOperator(
            task_id='extract_tech',
            python_callable=generate_tech_data,
        )
        extract_university = PythonOperator(
            task_id='extract_university',
            python_callable=generate_university_data,
        )
    
    # Validate: Run quality checks
    with TaskGroup(group_id='validate_group') as validate_group:
        validate_health = PythonOperator(
            task_id='validate_health',
            python_callable=validate_data,
            op_kwargs={'domain': 'health'},
        )
        validate_finance = PythonOperator(
            task_id='validate_finance',
            python_callable=validate_data,
            op_kwargs={'domain': 'finance'},
        )
        validate_tech = PythonOperator(
            task_id='validate_tech',
            python_callable=validate_data,
            op_kwargs={'domain': 'tech'},
        )
        validate_university = PythonOperator(
            task_id='validate_university',
            python_callable=validate_data,
            op_kwargs={'domain': 'university'},
        )
    
    # Quality gate
    quality_check = BranchPythonOperator(
        task_id='quality_check',
        python_callable=check_data_quality,
    )
    
    quality_alert = PythonOperator(
        task_id='quality_alert',
        python_callable=send_quality_alert,
    )
    
    # Transform: Apply transformations
    with TaskGroup(group_id='transform_group') as transform_group:
        transform_health = PythonOperator(
            task_id='transform_health',
            python_callable=transform_to_warehouse,
            op_kwargs={'domain': 'health'},
        )
        transform_finance = PythonOperator(
            task_id='transform_finance',
            python_callable=transform_to_warehouse,
            op_kwargs={'domain': 'finance'},
        )
        transform_tech = PythonOperator(
            task_id='transform_tech',
            python_callable=transform_to_warehouse,
            op_kwargs={'domain': 'tech'},
        )
        transform_university = PythonOperator(
            task_id='transform_university',
            python_callable=transform_to_warehouse,
            op_kwargs={'domain': 'university'},
        )
    
    # Generate reports
    reports = PythonOperator(
        task_id='generate_reports',
        python_callable=generate_reports,
        trigger_rule='none_failed_min_one_success',
    )
    
    # End
    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success',
    )
    
    # Define dependencies
    start >> extract_group >> validate_group >> quality_check
    quality_check >> [transform_group, quality_alert]
    transform_group >> reports >> end
    quality_alert >> end


# =============================================================================
# For local testing without Airflow
# =============================================================================

if __name__ == "__main__":
    print("Testing DAG tasks locally...")
    
    # Mock context
    class MockTI:
        def __init__(self):
            self.xcoms = {}
        def xcom_push(self, key, value):
            self.xcoms[key] = value
            print(f"  XCom: {key} = {value}")
        def xcom_pull(self, key, task_ids=None):
            return self.xcoms.get(key)
    
    context = {'ti': MockTI(), 'ds_nodash': '20240101'}
    
    print("\n1. Generating Health Data...")
    print(f"   Result: {generate_health_data(**context)}")
    
    print("\n2. Validating Health Data...")
    result = validate_data('health', **context)
    print(f"   Result: {result}")
    
    print("\n3. Transforming Health Data...")
    print(f"   Result: {transform_to_warehouse('health', **context)}")
    
    print("\nDAG test complete!")
