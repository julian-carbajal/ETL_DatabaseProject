"""
Health Domain Data Generator.
Generates realistic healthcare data including patients, diagnoses, medications, lab results.
"""

import random
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional

from .base import BaseDataGenerator


class HealthDataGenerator(BaseDataGenerator):
    """Generate realistic healthcare data."""
    
    def __init__(self, seed: Optional[int] = None):
        super().__init__(seed)
        
        # ICD-10 codes and descriptions
        self.icd10_codes = [
            ("E11.9", "Type 2 diabetes mellitus without complications"),
            ("I10", "Essential (primary) hypertension"),
            ("J06.9", "Acute upper respiratory infection, unspecified"),
            ("M54.5", "Low back pain"),
            ("F32.9", "Major depressive disorder, single episode, unspecified"),
            ("J45.909", "Unspecified asthma, uncomplicated"),
            ("K21.0", "Gastro-esophageal reflux disease with esophagitis"),
            ("G43.909", "Migraine, unspecified, not intractable"),
            ("N39.0", "Urinary tract infection, site not specified"),
            ("J02.9", "Acute pharyngitis, unspecified"),
            ("R51", "Headache"),
            ("M79.3", "Panniculitis, unspecified"),
            ("R10.9", "Unspecified abdominal pain"),
            ("R05", "Cough"),
            ("J20.9", "Acute bronchitis, unspecified"),
            ("E78.5", "Hyperlipidemia, unspecified"),
            ("F41.1", "Generalized anxiety disorder"),
            ("M25.50", "Pain in unspecified joint"),
            ("R50.9", "Fever, unspecified"),
            ("K30", "Functional dyspepsia"),
        ]
        
        # Common medications
        self.medications = [
            ("Metformin", "Metformin HCl", "500mg", "ORAL", "Twice daily"),
            ("Lisinopril", "Lisinopril", "10mg", "ORAL", "Once daily"),
            ("Atorvastatin", "Atorvastatin Calcium", "20mg", "ORAL", "Once daily at bedtime"),
            ("Omeprazole", "Omeprazole", "20mg", "ORAL", "Once daily before breakfast"),
            ("Amlodipine", "Amlodipine Besylate", "5mg", "ORAL", "Once daily"),
            ("Metoprolol", "Metoprolol Tartrate", "25mg", "ORAL", "Twice daily"),
            ("Albuterol", "Albuterol Sulfate", "90mcg", "INHALATION", "As needed"),
            ("Gabapentin", "Gabapentin", "300mg", "ORAL", "Three times daily"),
            ("Sertraline", "Sertraline HCl", "50mg", "ORAL", "Once daily"),
            ("Hydrochlorothiazide", "Hydrochlorothiazide", "25mg", "ORAL", "Once daily"),
            ("Levothyroxine", "Levothyroxine Sodium", "50mcg", "ORAL", "Once daily"),
            ("Prednisone", "Prednisone", "10mg", "ORAL", "As directed"),
            ("Amoxicillin", "Amoxicillin", "500mg", "ORAL", "Three times daily"),
            ("Ibuprofen", "Ibuprofen", "400mg", "ORAL", "Every 6 hours as needed"),
            ("Acetaminophen", "Acetaminophen", "500mg", "ORAL", "Every 4-6 hours as needed"),
        ]
        
        # Lab tests with LOINC codes
        self.lab_tests = [
            ("2345-7", "Glucose", "mg/dL", 70, 100, 65, 400),
            ("2093-3", "Cholesterol Total", "mg/dL", 125, 200, 100, 350),
            ("2571-8", "Triglycerides", "mg/dL", 40, 150, 30, 500),
            ("2085-9", "HDL Cholesterol", "mg/dL", 40, 60, 20, 100),
            ("13457-7", "LDL Cholesterol", "mg/dL", 50, 100, 40, 250),
            ("4548-4", "Hemoglobin A1c", "%", 4.0, 5.6, 3.5, 14.0),
            ("2160-0", "Creatinine", "mg/dL", 0.7, 1.3, 0.5, 10.0),
            ("3094-0", "BUN", "mg/dL", 7, 20, 5, 100),
            ("2951-2", "Sodium", "mEq/L", 136, 145, 120, 160),
            ("2823-3", "Potassium", "mEq/L", 3.5, 5.0, 2.5, 7.0),
            ("718-7", "Hemoglobin", "g/dL", 12.0, 17.5, 7.0, 20.0),
            ("787-2", "MCV", "fL", 80, 100, 60, 120),
            ("777-3", "Platelet Count", "10*3/uL", 150, 400, 50, 800),
            ("6690-2", "WBC", "10*3/uL", 4.5, 11.0, 1.0, 30.0),
            ("1742-6", "ALT", "U/L", 7, 56, 5, 500),
            ("1920-8", "AST", "U/L", 10, 40, 5, 500),
            ("1975-2", "Bilirubin Total", "mg/dL", 0.1, 1.2, 0.1, 15.0),
            ("2339-0", "Glucose Fasting", "mg/dL", 70, 100, 50, 400),
        ]
        
        # Provider specialties
        self.specialties = [
            "Internal Medicine", "Family Medicine", "Cardiology", "Endocrinology",
            "Pulmonology", "Gastroenterology", "Neurology", "Psychiatry",
            "Orthopedics", "Dermatology", "Oncology", "Nephrology",
            "Rheumatology", "Infectious Disease", "Emergency Medicine",
        ]
        
        # Blood types
        self.blood_types = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
        
        # Insurance providers
        self.insurance_providers = [
            "Blue Cross Blue Shield", "UnitedHealthcare", "Aetna", "Cigna",
            "Humana", "Kaiser Permanente", "Anthem", "Medicare", "Medicaid",
        ]
    
    def generate(self, count: int) -> List[Dict[str, Any]]:
        """Generate patient records."""
        return self.generate_patients(count)
    
    def generate_patients(self, count: int) -> List[Dict[str, Any]]:
        """Generate patient records."""
        patients = []
        
        for i in range(count):
            first_name, last_name = self.random_name()
            address = self.random_address()
            dob = self.random_date(
                date(1940, 1, 1),
                date(2010, 12, 31)
            )
            
            patient = {
                "patient_id": self.random_id("PAT"),
                "first_name": first_name,
                "last_name": last_name,
                "date_of_birth": dob.isoformat(),
                "gender": random.choice(["M", "F", "O"]),
                "ssn_hash": self.hash_value(self.random_ssn()),
                "email": self.random_email(first_name, last_name),
                "phone": self.random_phone(),
                **address,
                "blood_type": self.weighted_choice(
                    self.blood_types,
                    [0.34, 0.06, 0.09, 0.02, 0.03, 0.01, 0.38, 0.07]
                ),
                "primary_language": self.weighted_choice(
                    ["English", "Spanish", "Chinese", "Vietnamese", "Korean", "Other"],
                    [0.75, 0.13, 0.04, 0.02, 0.02, 0.04]
                ),
                "insurance_provider": random.choice(self.insurance_providers),
                "insurance_policy_number": self.random_id("POL", 10),
                "is_deceased": False,
                "created_at": datetime.utcnow().isoformat(),
            }
            patients.append(patient)
        
        return patients
    
    def generate_providers(self, count: int) -> List[Dict[str, Any]]:
        """Generate healthcare provider records."""
        providers = []
        credentials = ["MD", "DO", "NP", "PA"]
        ranks = ["Professor", "Associate Professor", "Assistant Professor", "Attending"]
        
        for i in range(count):
            first_name, last_name = self.random_name()
            specialty = random.choice(self.specialties)
            
            provider = {
                "provider_id": self.random_id("PRV"),
                "npi": ''.join(random.choices("0123456789", k=10)),
                "first_name": first_name,
                "last_name": last_name,
                "credentials": self.weighted_choice(credentials, [0.6, 0.15, 0.15, 0.1]),
                "specialty": specialty,
                "department": specialty,
                "email": f"{first_name.lower()}.{last_name.lower()}@hospital.org",
                "phone": self.random_phone(),
                "facility_name": random.choice([
                    "City General Hospital", "University Medical Center",
                    "Regional Health System", "Community Hospital",
                    "Memorial Medical Center", "St. Mary's Hospital",
                ]),
                "is_active": True,
                "license_number": self.random_id("LIC", 8),
                "license_state": random.choice(self.cities)[1],
                "license_expiry": self.random_date(
                    date.today(),
                    date.today() + timedelta(days=1095)
                ).isoformat(),
                "created_at": datetime.utcnow().isoformat(),
            }
            providers.append(provider)
        
        return providers
    
    def generate_diagnoses(
        self,
        patient_ids: List[str],
        provider_ids: List[str],
        count: int
    ) -> List[Dict[str, Any]]:
        """Generate diagnosis records."""
        diagnoses = []
        
        for i in range(count):
            icd10_code, description = random.choice(self.icd10_codes)
            diagnosis_date = self.random_date(
                date.today() - timedelta(days=365*2),
                date.today()
            )
            
            diagnosis = {
                "diagnosis_id": self.random_id("DX"),
                "patient_id": random.choice(patient_ids),
                "provider_id": random.choice(provider_ids),
                "icd10_code": icd10_code,
                "icd10_description": description,
                "diagnosis_date": diagnosis_date.isoformat(),
                "status": self.weighted_choice(
                    ["ACTIVE", "RESOLVED", "CHRONIC", "RULED_OUT"],
                    [0.4, 0.3, 0.25, 0.05]
                ),
                "severity": self.weighted_choice(
                    ["MILD", "MODERATE", "SEVERE"],
                    [0.5, 0.35, 0.15]
                ),
                "is_primary": random.random() < 0.3,
                "created_at": datetime.utcnow().isoformat(),
            }
            diagnoses.append(diagnosis)
        
        return diagnoses
    
    def generate_medications(
        self,
        patient_ids: List[str],
        provider_ids: List[str],
        count: int
    ) -> List[Dict[str, Any]]:
        """Generate medication records."""
        medications = []
        
        for i in range(count):
            med = random.choice(self.medications)
            drug_name, generic_name, dosage, route, frequency = med
            start_date = self.random_date(
                date.today() - timedelta(days=365),
                date.today()
            )
            
            medication = {
                "medication_id": self.random_id("MED"),
                "patient_id": random.choice(patient_ids),
                "prescriber_id": random.choice(provider_ids),
                "drug_name": drug_name,
                "generic_name": generic_name,
                "dosage": dosage,
                "route": route,
                "frequency": frequency,
                "start_date": start_date.isoformat(),
                "end_date": (
                    (start_date + timedelta(days=random.randint(30, 365))).isoformat()
                    if random.random() < 0.3 else None
                ),
                "is_active": random.random() > 0.3,
                "quantity": random.choice([30, 60, 90]),
                "refills_remaining": random.randint(0, 5),
                "days_supply": random.choice([30, 60, 90]),
                "created_at": datetime.utcnow().isoformat(),
            }
            medications.append(medication)
        
        return medications
    
    def generate_lab_results(
        self,
        patient_ids: List[str],
        provider_ids: List[str],
        count: int
    ) -> List[Dict[str, Any]]:
        """Generate lab result records."""
        results = []
        
        for i in range(count):
            test = random.choice(self.lab_tests)
            loinc, name, unit, ref_low, ref_high, abs_low, abs_high = test
            
            # Generate result value (sometimes abnormal)
            if random.random() < 0.2:  # 20% abnormal
                if random.random() < 0.5:
                    value = random.uniform(abs_low, ref_low)
                else:
                    value = random.uniform(ref_high, abs_high)
            else:
                value = random.uniform(ref_low, ref_high)
            
            result_date = self.random_datetime(
                datetime.now() - timedelta(days=365),
                datetime.now()
            )
            
            # Determine abnormal flag
            abnormal_flag = None
            if value < ref_low:
                abnormal_flag = "LL" if value < ref_low * 0.8 else "L"
            elif value > ref_high:
                abnormal_flag = "HH" if value > ref_high * 1.2 else "H"
            
            lab_result = {
                "lab_result_id": self.random_id("LAB"),
                "patient_id": random.choice(patient_ids),
                "ordering_provider_id": random.choice(provider_ids),
                "test_name": name,
                "loinc_code": loinc,
                "result_value": str(round(value, 2)),
                "result_numeric": round(value, 2),
                "result_unit": unit,
                "reference_range_low": ref_low,
                "reference_range_high": ref_high,
                "result_status": "FINAL",
                "abnormal_flag": abnormal_flag,
                "critical_flag": abnormal_flag in ["LL", "HH"],
                "result_date": result_date.isoformat(),
                "collection_date": (result_date - timedelta(hours=random.randint(1, 24))).isoformat(),
                "performing_lab": random.choice([
                    "Quest Diagnostics", "LabCorp", "Hospital Lab", "BioReference"
                ]),
                "specimen_type": random.choice(["Blood", "Serum", "Plasma", "Urine"]),
                "created_at": datetime.utcnow().isoformat(),
            }
            results.append(lab_result)
        
        return results
    
    def generate_encounters(
        self,
        patient_ids: List[str],
        provider_ids: List[str],
        count: int
    ) -> List[Dict[str, Any]]:
        """Generate medical encounter records."""
        encounters = []
        encounter_types = ["OUTPATIENT", "INPATIENT", "EMERGENCY", "TELEHEALTH"]
        
        for i in range(count):
            enc_type = self.weighted_choice(
                encounter_types,
                [0.6, 0.15, 0.1, 0.15]
            )
            
            enc_date = self.random_datetime(
                datetime.now() - timedelta(days=365),
                datetime.now()
            )
            
            # Generate vitals
            age_factor = random.uniform(0.8, 1.2)
            
            encounter = {
                "encounter_id": self.random_id("ENC"),
                "patient_id": random.choice(patient_ids),
                "provider_id": random.choice(provider_ids),
                "encounter_type": enc_type,
                "encounter_date": enc_date.isoformat(),
                "discharge_date": (
                    (enc_date + timedelta(days=random.randint(1, 7))).isoformat()
                    if enc_type == "INPATIENT" else enc_date.isoformat()
                ),
                "facility_name": random.choice([
                    "City General Hospital", "University Medical Center",
                    "Regional Health System", "Urgent Care Center",
                ]),
                "department": random.choice(self.specialties),
                "chief_complaint": random.choice([
                    "Chest pain", "Shortness of breath", "Abdominal pain",
                    "Headache", "Back pain", "Fever", "Cough", "Fatigue",
                    "Dizziness", "Nausea", "Annual checkup", "Follow-up visit",
                ]),
                "blood_pressure_systolic": int(120 * age_factor + random.randint(-20, 40)),
                "blood_pressure_diastolic": int(80 * age_factor + random.randint(-10, 20)),
                "heart_rate": int(72 * age_factor + random.randint(-15, 30)),
                "temperature": round(98.6 + random.uniform(-0.5, 2.0), 1),
                "respiratory_rate": random.randint(12, 20),
                "oxygen_saturation": round(random.uniform(94, 100), 1),
                "weight_kg": round(random.uniform(50, 120), 1),
                "height_cm": round(random.uniform(150, 195), 1),
                "total_charges": round(random.uniform(100, 50000), 2),
                "status": "DISCHARGED" if enc_date < datetime.now() - timedelta(days=1) else "ACTIVE",
                "created_at": datetime.utcnow().isoformat(),
            }
            encounters.append(encounter)
        
        return encounters
    
    def generate_complete_dataset(
        self,
        num_patients: int = 100,
        num_providers: int = 20,
        diagnoses_per_patient: float = 3.0,
        medications_per_patient: float = 2.5,
        labs_per_patient: float = 5.0,
        encounters_per_patient: float = 4.0,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Generate a complete healthcare dataset."""
        
        # Generate base entities
        patients = self.generate_patients(num_patients)
        providers = self.generate_providers(num_providers)
        
        patient_ids = [p["patient_id"] for p in patients]
        provider_ids = [p["provider_id"] for p in providers]
        
        # Generate related records
        num_diagnoses = int(num_patients * diagnoses_per_patient)
        num_medications = int(num_patients * medications_per_patient)
        num_labs = int(num_patients * labs_per_patient)
        num_encounters = int(num_patients * encounters_per_patient)
        
        return {
            "patients": patients,
            "providers": providers,
            "diagnoses": self.generate_diagnoses(patient_ids, provider_ids, num_diagnoses),
            "medications": self.generate_medications(patient_ids, provider_ids, num_medications),
            "lab_results": self.generate_lab_results(patient_ids, provider_ids, num_labs),
            "encounters": self.generate_encounters(patient_ids, provider_ids, num_encounters),
        }
