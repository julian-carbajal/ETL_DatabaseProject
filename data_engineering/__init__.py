"""
Enterprise Data Engineering Platform
=====================================

A comprehensive data engineering project spanning Health, Tech, Finance, and University domains.

Modules:
    - config: Configuration and settings
    - models: SQLAlchemy data models for all domains
    - generators: Synthetic data generators
    - etl: ETL pipeline framework (Extract, Transform, Load)
    - quality: Data quality validation, profiling, and monitoring
    - analytics: Aggregations, metrics, and reporting

Author: Julian Carbajal
Version: 1.0.0
"""

__version__ = "1.0.0"
__author__ = "Julian Carbajal"

from . import config
from . import models
from . import generators
from . import etl
from . import quality
from . import analytics
