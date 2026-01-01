"""Data models for all domains."""

from .base import Base, TimestampMixin, AuditMixin
from .health import Patient, Diagnosis, Medication, LabResult, HealthcareProvider, MedicalEncounter
from .finance import Account, Transaction, Investment, LoanApplication, CreditScore, FinancialStatement
from .tech import Server, Application, Incident, Deployment, MetricLog, APIEndpoint
from .university import Student, Faculty, Course, Enrollment, Department, ResearchGrant

__all__ = [
    # Base
    "Base", "TimestampMixin", "AuditMixin",
    # Health
    "Patient", "Diagnosis", "Medication", "LabResult", "HealthcareProvider", "MedicalEncounter",
    # Finance
    "Account", "Transaction", "Investment", "LoanApplication", "CreditScore", "FinancialStatement",
    # Tech
    "Server", "Application", "Incident", "Deployment", "MetricLog", "APIEndpoint",
    # University
    "Student", "Faculty", "Course", "Enrollment", "Department", "ResearchGrant",
]
