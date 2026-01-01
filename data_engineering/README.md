# Enterprise Data Engineering Platform

A comprehensive, production-grade data engineering project spanning **Health**, **Tech**, **Finance**, and **University** domains. This project demonstrates real-world data engineering skills and practices used across multiple industries.

## 🎯 Project Overview

This platform showcases the complete data engineering lifecycle:

- **ETL Pipeline Development** - Extract, Transform, Load with orchestration
- **Data Modeling** - Domain-specific schemas with SQLAlchemy ORM
- **Data Quality Management** - Validation, profiling, and monitoring
- **Analytics & Reporting** - KPIs, aggregations, and automated reports
- **Compliance Patterns** - HIPAA, SOX, and audit trail implementations

## 🏗️ Architecture

```
data_engineering/
├── config/                 # Configuration and settings
│   ├── __init__.py
│   └── settings.py         # Database, ETL, and domain configs
│
├── models/                 # SQLAlchemy Data Models
│   ├── __init__.py
│   ├── base.py            # Base classes and mixins
│   ├── health.py          # Healthcare domain (HIPAA compliant)
│   ├── finance.py         # Financial domain (SOX compliant)
│   ├── tech.py            # Infrastructure monitoring
│   └── university.py      # Academic data management
│
├── generators/            # Synthetic Data Generators
│   ├── __init__.py
│   ├── base.py            # Base generator utilities
│   ├── health_generator.py
│   ├── finance_generator.py
│   ├── tech_generator.py
│   └── university_generator.py
│
├── etl/                   # ETL Pipeline Framework
│   ├── __init__.py
│   ├── base.py            # Pipeline, Step, Context classes
│   ├── extractors.py      # CSV, JSON, API, Database extractors
│   ├── transformers.py    # Cleansing, validation, enrichment
│   ├── loaders.py         # Database, file, SCD2 loaders
│   └── orchestrator.py    # Pipeline orchestration and scheduling
│
├── quality/               # Data Quality Framework
│   ├── __init__.py
│   ├── validators.py      # Schema and business rule validation
│   ├── profiler.py        # Data profiling and statistics
│   └── monitors.py        # Quality monitoring and alerting
│
├── analytics/             # Analytics and Reporting
│   ├── __init__.py
│   ├── aggregations.py    # Data aggregation and metrics
│   └── reports.py         # Report generation (HTML, Markdown)
│
├── data/                  # Data Storage (generated)
│   ├── raw/               # Raw extracted data
│   ├── processed/         # Transformed data
│   ├── warehouse/         # Data warehouse
│   └── reports/           # Generated reports
│
├── main.py                # Main runner script
├── requirements.txt       # Python dependencies
└── README.md              # This file
```

## 🏥 Health Domain

HIPAA-compliant healthcare data models and processing:

| Entity | Description | Key Features |
|--------|-------------|--------------|
| Patient | Patient master record | PHI encryption, demographics |
| Provider | Healthcare providers | NPI, credentials, specialties |
| Diagnosis | ICD-10 coded diagnoses | Status tracking, severity |
| Medication | Prescription records | NDC codes, refill tracking |
| LabResult | Laboratory results | LOINC codes, reference ranges |
| Encounter | Medical visits | Vitals, billing, discharge |

**Key Metrics:**
- Patient demographics and age distribution
- Encounter volume by type
- Abnormal/critical lab result rates
- Average encounter charges

## 💰 Finance Domain

SOX-compliant financial data models:

| Entity | Description | Key Features |
|--------|-------------|--------------|
| Account | Financial accounts | Multi-type, KYC/AML tracking |
| Transaction | Transaction records | Fraud scoring, immutable audit |
| Investment | Portfolio positions | Real-time valuation |
| LoanApplication | Loan underwriting | Credit assessment |
| CreditScore | Credit history | Multi-bureau tracking |
| FinancialStatement | Company financials | Ratios, audit opinions |

**Key Metrics:**
- Account balances and distribution
- Transaction volume and fraud rates
- Loan approval rates
- Portfolio performance

## 🖥️ Tech Domain

Infrastructure and application monitoring:

| Entity | Description | Key Features |
|--------|-------------|--------------|
| Server | Server inventory | Multi-cloud, specs, status |
| Application | App registry | Dependencies, health checks |
| Incident | Incident management | Severity, MTTR tracking |
| Deployment | Deployment history | CI/CD metrics |
| MetricLog | Time-series metrics | CPU, memory, disk, network |
| APIEndpoint | API registry | SLAs, performance |

**Key Metrics:**
- Server availability rates
- Incident MTTR (Mean Time To Resolve)
- Deployment success rates
- Resource utilization

## 🎓 University Domain

Academic data management:

| Entity | Description | Key Features |
|--------|-------------|--------------|
| Student | Student records | GPA, enrollment status |
| Faculty | Faculty members | Rank, publications, tenure |
| Department | Academic departments | Budget, accreditation |
| Course | Course catalog | Sections, enrollment |
| Enrollment | Student enrollments | Grades, attendance |
| ResearchGrant | Research funding | PI, funding agency |

**Key Metrics:**
- Student retention rates
- Average GPA and distribution
- Course fill rates
- Research funding totals

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- pip

### Installation

```bash
# Navigate to project directory
cd data_engineering

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Running the Platform

```bash
# Run full demonstration
python main.py

# With custom seed for reproducibility
python main.py --seed 123

# Skip report generation
python main.py --skip-reports

# Skip ETL demonstration
python main.py --skip-etl
```

### Output

The platform will:
1. Generate synthetic data for all 4 domains
2. Save raw data to `data/raw/`
3. Run data quality checks and profiling
4. Calculate domain-specific metrics
5. Generate HTML and Markdown reports in `data/reports/`
6. Demonstrate ETL pipeline capabilities

## 📊 Data Quality Framework

### Validation

```python
from quality.validators import DataValidator, create_health_validator

# Use pre-built domain validator
validator = create_health_validator()
report = validator.validate(patient_data, id_field="patient_id")

print(f"Validity Rate: {report.validity_rate:.1%}")
print(f"Errors: {report.error_count}")
```

### Profiling

```python
from quality.profiler import DataProfiler, DataQualityScorer

profiler = DataProfiler()
profile = profiler.profile(data, "my_dataset")

print(profile.summary())

scorer = DataQualityScorer()
score = scorer.score(profile)
print(f"Quality Score: {score['overall_score']}/100 ({score['grade']})")
```

### Monitoring

```python
from quality.monitors import DataQualityMonitor, AlertSeverity

monitor = DataQualityMonitor("MyMonitor")
monitor.add_completeness_rule("email", min_completeness=0.99)
monitor.add_uniqueness_rule("id", min_uniqueness=1.0)
monitor.add_row_count_rule(min_rows=1000)

result = monitor.monitor(data, "daily_data")
print(f"Alerts: {result['alert_count']}")
```

## 🔄 ETL Pipeline Framework

### Building Pipelines

```python
from etl.base import ETLPipeline, ETLContext
from etl.transformers import CleansingTransformer, EnrichmentTransformer

# Create pipeline
pipeline = ETLPipeline(
    name="CustomerPipeline",
    max_retries=3
)

# Add transformation steps
pipeline.add_step(CleansingTransformer(
    trim_strings=True,
    lowercase_columns=["email"]
))

pipeline.add_step(EnrichmentTransformer(
    computed_fields={
        "full_name": lambda r: f"{r['first']} {r['last']}"
    }
))

# Run pipeline
context = ETLContext()
context.raw_data = my_data
result = pipeline.run(context)

print(f"Processed: {result.metrics.records_transformed}")
```

### Pipeline Orchestration

```python
from etl.orchestrator import WorkflowBuilder

workflow = (WorkflowBuilder("DailyETL")
    .add_job("extract_customers", customer_pipeline)
    .add_job("extract_orders", order_pipeline)
    .add_job("transform_data", transform_pipeline)
        .depends_on("extract_customers", "extract_orders")
    .add_job("load_warehouse", load_pipeline)
        .depends_on("transform_data")
    .build()
)

results = workflow.run_all()
```

## 📈 Analytics & Reporting

### Calculating Metrics

```python
from analytics.aggregations import MetricsCalculator

calculator = MetricsCalculator()

# Domain-specific metrics
health_metrics = calculator.calculate_health_metrics(health_data)
finance_metrics = calculator.calculate_finance_metrics(finance_data)
tech_metrics = calculator.calculate_tech_metrics(tech_data)
uni_metrics = calculator.calculate_university_metrics(university_data)
```

### Generating Reports

```python
from analytics.reports import ReportGenerator

generator = ReportGenerator()

report = generator.generate_health_report(data, metrics)
report.save("reports/health_report.html", format="html")
report.save("reports/health_report.md", format="markdown")
```

## 🧪 Testing

```bash
# Run tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=data_engineering --cov-report=html
```

## 📚 Skills Demonstrated

This project demonstrates proficiency in:

### Data Engineering
- ETL pipeline design and implementation
- Data modeling (Star/Snowflake schemas)
- Batch and streaming data processing
- Data quality management
- Data lineage tracking

### Domain Knowledge
- **Healthcare**: HIPAA compliance, ICD-10, LOINC, HL7
- **Finance**: SOX compliance, AML/KYC, transaction processing
- **Tech**: Infrastructure monitoring, SRE practices, CI/CD
- **Academic**: Student information systems, research management

### Technical Skills
- Python (OOP, type hints, dataclasses)
- SQLAlchemy ORM
- Data validation and profiling
- Report generation
- Logging and monitoring

### Best Practices
- Clean code architecture
- Configuration management
- Error handling and retry logic
- Audit trails and compliance
- Documentation

## 🔮 Future Enhancements

- [ ] Apache Airflow integration for scheduling
- [ ] Real-time streaming with Apache Kafka
- [ ] REST API with FastAPI
- [ ] Docker containerization
- [ ] Kubernetes deployment
- [ ] Data visualization dashboard
- [ ] Machine learning pipeline integration
- [ ] Cloud deployment (AWS/GCP/Azure)

## 📄 License

This project is for educational and portfolio purposes.

## 👤 Author

**Julian Carbajal**
- Chapman University
- CPSC-350

---

*Built as an advanced database engineering project to demonstrate comprehensive data engineering skills across multiple industry domains.*
