"""
Health Domain Models - HIPAA Compliant Healthcare Data Models.
Covers: Patient records, diagnoses, medications, lab results, providers, encounters.
"""

from datetime import datetime, date
from typing import Optional, List
from sqlalchemy import (
    Column, Integer, String, DateTime, Date, Float, Boolean, 
    Text, ForeignKey, Numeric, Enum, Index, CheckConstraint
)
from sqlalchemy.orm import relationship
import enum

from .base import Base, TimestampMixin, AuditMixin, DataLineageMixin, EncryptionMixin


class Gender(enum.Enum):
    MALE = "M"
    FEMALE = "F"
    OTHER = "O"
    UNKNOWN = "U"


class EncounterType(enum.Enum):
    INPATIENT = "INPATIENT"
    OUTPATIENT = "OUTPATIENT"
    EMERGENCY = "EMERGENCY"
    TELEHEALTH = "TELEHEALTH"
    HOME_VISIT = "HOME_VISIT"


class DiagnosisStatus(enum.Enum):
    ACTIVE = "ACTIVE"
    RESOLVED = "RESOLVED"
    CHRONIC = "CHRONIC"
    RULED_OUT = "RULED_OUT"


class Patient(Base, TimestampMixin, AuditMixin, DataLineageMixin, EncryptionMixin):
    """
    Patient master record - Central entity for healthcare domain.
    Contains PHI (Protected Health Information) requiring HIPAA compliance.
    """
    __tablename__ = "health_patients"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    patient_id = Column(String(20), unique=True, nullable=False, index=True)  # PAT00000001
    
    # Demographics (PHI - requires encryption)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    date_of_birth = Column(Date, nullable=False)
    gender = Column(String(10), nullable=True)
    ssn_hash = Column(String(256), nullable=True)  # Hashed SSN for matching
    
    # Contact Information (PHI)
    email = Column(String(255), nullable=True)
    phone = Column(String(20), nullable=True)
    address_line1 = Column(String(255), nullable=True)
    address_line2 = Column(String(255), nullable=True)
    city = Column(String(100), nullable=True)
    state = Column(String(50), nullable=True)
    zip_code = Column(String(20), nullable=True)
    country = Column(String(100), default="USA")
    
    # Medical Information
    blood_type = Column(String(10), nullable=True)
    primary_language = Column(String(50), default="English")
    emergency_contact_name = Column(String(200), nullable=True)
    emergency_contact_phone = Column(String(20), nullable=True)
    
    # Insurance Information
    insurance_provider = Column(String(200), nullable=True)
    insurance_policy_number = Column(String(100), nullable=True)
    insurance_group_number = Column(String(100), nullable=True)
    
    # Status
    is_deceased = Column(Boolean, default=False)
    deceased_date = Column(Date, nullable=True)
    
    # Relationships
    diagnoses = relationship("Diagnosis", back_populates="patient", lazy="dynamic")
    medications = relationship("Medication", back_populates="patient", lazy="dynamic")
    lab_results = relationship("LabResult", back_populates="patient", lazy="dynamic")
    encounters = relationship("MedicalEncounter", back_populates="patient", lazy="dynamic")
    
    __table_args__ = (
        Index("idx_patient_name", "last_name", "first_name"),
        Index("idx_patient_dob", "date_of_birth"),
        CheckConstraint("date_of_birth <= CURRENT_DATE", name="check_valid_dob"),
    )
    
    def __repr__(self):
        return f"<Patient(id={self.patient_id}, name={self.last_name}, {self.first_name})>"
    
    @property
    def age(self) -> int:
        """Calculate patient age."""
        today = date.today()
        return today.year - self.date_of_birth.year - (
            (today.month, today.day) < (self.date_of_birth.month, self.date_of_birth.day)
        )


class HealthcareProvider(Base, TimestampMixin, AuditMixin):
    """Healthcare provider (doctor, nurse, specialist) information."""
    __tablename__ = "health_providers"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    provider_id = Column(String(20), unique=True, nullable=False, index=True)  # PRV00000001
    npi = Column(String(10), unique=True, nullable=True)  # National Provider Identifier
    
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    credentials = Column(String(50), nullable=True)  # MD, DO, NP, PA, RN
    specialty = Column(String(100), nullable=True)
    department = Column(String(100), nullable=True)
    
    email = Column(String(255), nullable=True)
    phone = Column(String(20), nullable=True)
    
    facility_name = Column(String(200), nullable=True)
    facility_address = Column(String(500), nullable=True)
    
    is_active = Column(Boolean, default=True)
    license_number = Column(String(50), nullable=True)
    license_state = Column(String(50), nullable=True)
    license_expiry = Column(Date, nullable=True)
    
    # Relationships
    diagnoses = relationship("Diagnosis", back_populates="provider", lazy="dynamic")
    encounters = relationship("MedicalEncounter", back_populates="provider", lazy="dynamic")
    
    def __repr__(self):
        return f"<Provider(id={self.provider_id}, name=Dr. {self.last_name})>"


class Diagnosis(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """Patient diagnosis records with ICD-10 coding."""
    __tablename__ = "health_diagnoses"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    diagnosis_id = Column(String(20), unique=True, nullable=False, index=True)
    
    patient_id = Column(Integer, ForeignKey("health_patients.id"), nullable=False)
    provider_id = Column(Integer, ForeignKey("health_providers.id"), nullable=True)
    encounter_id = Column(Integer, ForeignKey("health_encounters.id"), nullable=True)
    
    # ICD-10 Coding
    icd10_code = Column(String(20), nullable=False, index=True)
    icd10_description = Column(String(500), nullable=True)
    
    # Diagnosis Details
    diagnosis_date = Column(Date, nullable=False)
    status = Column(String(20), default="ACTIVE")
    severity = Column(String(20), nullable=True)  # MILD, MODERATE, SEVERE
    is_primary = Column(Boolean, default=False)
    
    # Clinical Notes
    clinical_notes = Column(Text, nullable=True)
    
    # Relationships
    patient = relationship("Patient", back_populates="diagnoses")
    provider = relationship("HealthcareProvider", back_populates="diagnoses")
    encounter = relationship("MedicalEncounter", back_populates="diagnoses")
    
    __table_args__ = (
        Index("idx_diagnosis_patient_date", "patient_id", "diagnosis_date"),
        Index("idx_diagnosis_icd10", "icd10_code"),
    )
    
    def __repr__(self):
        return f"<Diagnosis(id={self.diagnosis_id}, icd10={self.icd10_code})>"


class Medication(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """Patient medication prescriptions and history."""
    __tablename__ = "health_medications"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    medication_id = Column(String(20), unique=True, nullable=False, index=True)
    
    patient_id = Column(Integer, ForeignKey("health_patients.id"), nullable=False)
    prescriber_id = Column(Integer, ForeignKey("health_providers.id"), nullable=True)
    
    # Drug Information
    drug_name = Column(String(200), nullable=False)
    generic_name = Column(String(200), nullable=True)
    ndc_code = Column(String(20), nullable=True)  # National Drug Code
    rxnorm_code = Column(String(20), nullable=True)
    
    # Prescription Details
    dosage = Column(String(100), nullable=True)
    dosage_unit = Column(String(50), nullable=True)
    frequency = Column(String(100), nullable=True)
    route = Column(String(50), nullable=True)  # ORAL, IV, TOPICAL, etc.
    
    # Duration
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=True)
    is_active = Column(Boolean, default=True)
    
    # Refill Information
    quantity = Column(Integer, nullable=True)
    refills_remaining = Column(Integer, default=0)
    days_supply = Column(Integer, nullable=True)
    
    # Pharmacy
    pharmacy_name = Column(String(200), nullable=True)
    pharmacy_phone = Column(String(20), nullable=True)
    
    # Clinical
    indication = Column(String(500), nullable=True)
    special_instructions = Column(Text, nullable=True)
    
    # Relationships
    patient = relationship("Patient", back_populates="medications")
    prescriber = relationship("HealthcareProvider")
    
    __table_args__ = (
        Index("idx_medication_patient", "patient_id", "is_active"),
        Index("idx_medication_drug", "drug_name"),
    )
    
    def __repr__(self):
        return f"<Medication(id={self.medication_id}, drug={self.drug_name})>"


class LabResult(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """Laboratory test results with LOINC coding."""
    __tablename__ = "health_lab_results"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    lab_result_id = Column(String(20), unique=True, nullable=False, index=True)
    
    patient_id = Column(Integer, ForeignKey("health_patients.id"), nullable=False)
    ordering_provider_id = Column(Integer, ForeignKey("health_providers.id"), nullable=True)
    
    # Test Information
    test_name = Column(String(200), nullable=False)
    loinc_code = Column(String(20), nullable=True, index=True)  # LOINC standard code
    
    # Results
    result_value = Column(String(100), nullable=True)
    result_numeric = Column(Numeric(18, 6), nullable=True)
    result_unit = Column(String(50), nullable=True)
    reference_range_low = Column(Numeric(18, 6), nullable=True)
    reference_range_high = Column(Numeric(18, 6), nullable=True)
    
    # Status and Flags
    result_status = Column(String(20), default="FINAL")  # PRELIMINARY, FINAL, CORRECTED
    abnormal_flag = Column(String(10), nullable=True)  # H, L, HH, LL, N
    critical_flag = Column(Boolean, default=False)
    
    # Timestamps
    order_date = Column(DateTime, nullable=True)
    collection_date = Column(DateTime, nullable=True)
    result_date = Column(DateTime, nullable=False)
    
    # Lab Information
    performing_lab = Column(String(200), nullable=True)
    specimen_type = Column(String(100), nullable=True)
    
    # Clinical Notes
    interpretation = Column(Text, nullable=True)
    
    # Relationships
    patient = relationship("Patient", back_populates="lab_results")
    ordering_provider = relationship("HealthcareProvider")
    
    __table_args__ = (
        Index("idx_lab_patient_date", "patient_id", "result_date"),
        Index("idx_lab_test", "test_name", "result_date"),
    )
    
    def __repr__(self):
        return f"<LabResult(id={self.lab_result_id}, test={self.test_name})>"
    
    @property
    def is_abnormal(self) -> bool:
        """Check if result is outside reference range."""
        if self.result_numeric is None:
            return self.abnormal_flag is not None
        if self.reference_range_low and self.result_numeric < self.reference_range_low:
            return True
        if self.reference_range_high and self.result_numeric > self.reference_range_high:
            return True
        return False


class MedicalEncounter(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """Medical encounter/visit records."""
    __tablename__ = "health_encounters"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    encounter_id = Column(String(20), unique=True, nullable=False, index=True)
    
    patient_id = Column(Integer, ForeignKey("health_patients.id"), nullable=False)
    provider_id = Column(Integer, ForeignKey("health_providers.id"), nullable=True)
    
    # Encounter Details
    encounter_type = Column(String(20), nullable=False)  # INPATIENT, OUTPATIENT, EMERGENCY
    encounter_date = Column(DateTime, nullable=False)
    discharge_date = Column(DateTime, nullable=True)
    
    # Location
    facility_name = Column(String(200), nullable=True)
    department = Column(String(100), nullable=True)
    room_number = Column(String(20), nullable=True)
    
    # Chief Complaint and Notes
    chief_complaint = Column(Text, nullable=True)
    clinical_notes = Column(Text, nullable=True)
    discharge_summary = Column(Text, nullable=True)
    
    # Vitals at Encounter
    blood_pressure_systolic = Column(Integer, nullable=True)
    blood_pressure_diastolic = Column(Integer, nullable=True)
    heart_rate = Column(Integer, nullable=True)
    temperature = Column(Numeric(5, 2), nullable=True)
    respiratory_rate = Column(Integer, nullable=True)
    oxygen_saturation = Column(Numeric(5, 2), nullable=True)
    weight_kg = Column(Numeric(6, 2), nullable=True)
    height_cm = Column(Numeric(5, 1), nullable=True)
    
    # Billing
    total_charges = Column(Numeric(12, 2), nullable=True)
    insurance_paid = Column(Numeric(12, 2), nullable=True)
    patient_paid = Column(Numeric(12, 2), nullable=True)
    
    # Status
    status = Column(String(20), default="ACTIVE")  # ACTIVE, DISCHARGED, CANCELLED
    
    # Relationships
    patient = relationship("Patient", back_populates="encounters")
    provider = relationship("HealthcareProvider", back_populates="encounters")
    diagnoses = relationship("Diagnosis", back_populates="encounter", lazy="dynamic")
    
    __table_args__ = (
        Index("idx_encounter_patient_date", "patient_id", "encounter_date"),
        Index("idx_encounter_type_date", "encounter_type", "encounter_date"),
    )
    
    def __repr__(self):
        return f"<Encounter(id={self.encounter_id}, type={self.encounter_type})>"
    
    @property
    def length_of_stay_days(self) -> Optional[int]:
        """Calculate length of stay for inpatient encounters."""
        if self.discharge_date and self.encounter_date:
            return (self.discharge_date - self.encounter_date).days
        return None
