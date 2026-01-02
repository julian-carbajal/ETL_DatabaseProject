"""
Data Warehouse Builder
======================

Builds and populates data warehouse from source data.
Handles dimension loading, fact table population, and SCD management.
"""

import json
import hashlib
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import logging

from .schema import (
    StarSchema, DimensionTable, FactTable, SCDType,
    DateDimension, TimeDimension,
    create_health_star_schema, create_finance_star_schema,
    create_tech_star_schema, create_university_star_schema
)

logger = logging.getLogger(__name__)


class DimensionLoader:
    """
    Loads data into dimension tables with SCD support.
    """
    
    def __init__(self, dimension: DimensionTable):
        self.dimension = dimension
        self.data: List[Dict[str, Any]] = []
        self.lookup: Dict[str, int] = {}  # natural_key -> surrogate_key
        self._next_sk = 1
    
    def load(
        self,
        records: List[Dict[str, Any]],
        key_mapping: Dict[str, str] = None,
    ) -> int:
        """
        Load records into dimension.
        
        Args:
            records: Source records
            key_mapping: Map source field names to dimension column names
        
        Returns:
            Number of records loaded
        """
        key_mapping = key_mapping or {}
        loaded = 0
        
        for record in records:
            # Map source fields to dimension columns
            dim_record = {}
            for col in self.dimension.columns:
                source_field = key_mapping.get(col.name, col.name)
                if source_field in record:
                    dim_record[col.name] = record[source_field]
                elif col.default is not None:
                    dim_record[col.name] = col.default
            
            # Get natural key value
            natural_key = self._get_natural_key(dim_record)
            
            if self.dimension.scd_type == SCDType.TYPE_1:
                loaded += self._load_type1(dim_record, natural_key)
            elif self.dimension.scd_type == SCDType.TYPE_2:
                loaded += self._load_type2(dim_record, natural_key)
            else:
                loaded += self._load_type1(dim_record, natural_key)
        
        return loaded
    
    def _get_natural_key(self, record: Dict[str, Any]) -> str:
        """Get natural key value from record."""
        key_values = [
            str(record.get(k, ""))
            for k in self.dimension.natural_key
        ]
        return "|".join(key_values)
    
    def _load_type1(self, record: Dict[str, Any], natural_key: str) -> int:
        """Load with SCD Type 1 (overwrite)."""
        if natural_key in self.lookup:
            # Update existing record
            sk = self.lookup[natural_key]
            for i, existing in enumerate(self.data):
                if existing[self.dimension.surrogate_key] == sk:
                    record[self.dimension.surrogate_key] = sk
                    self.data[i] = record
                    return 0  # Updated, not new
        else:
            # Insert new record
            record[self.dimension.surrogate_key] = self._next_sk
            self.lookup[natural_key] = self._next_sk
            self._next_sk += 1
            self.data.append(record)
            return 1
        
        return 0
    
    def _load_type2(self, record: Dict[str, Any], natural_key: str) -> int:
        """Load with SCD Type 2 (add new row)."""
        today = date.today().isoformat()
        
        if natural_key in self.lookup:
            # Check if record has changed
            current_sk = self.lookup[natural_key]
            current_record = None
            
            for existing in self.data:
                if (existing[self.dimension.surrogate_key] == current_sk and
                    existing.get(self.dimension.is_current_column, True)):
                    current_record = existing
                    break
            
            if current_record:
                # Check for changes (excluding SCD columns)
                changed = False
                for col in self.dimension.columns:
                    if col.name in [
                        self.dimension.surrogate_key,
                        self.dimension.effective_date_column,
                        self.dimension.expiration_date_column,
                        self.dimension.is_current_column,
                    ]:
                        continue
                    if record.get(col.name) != current_record.get(col.name):
                        changed = True
                        break
                
                if changed:
                    # Expire current record
                    current_record[self.dimension.expiration_date_column] = today
                    current_record[self.dimension.is_current_column] = False
                    
                    # Insert new version
                    record[self.dimension.surrogate_key] = self._next_sk
                    record[self.dimension.effective_date_column] = today
                    record[self.dimension.expiration_date_column] = None
                    record[self.dimension.is_current_column] = True
                    
                    self.lookup[natural_key] = self._next_sk
                    self._next_sk += 1
                    self.data.append(record)
                    return 1
        else:
            # Insert new record
            record[self.dimension.surrogate_key] = self._next_sk
            record[self.dimension.effective_date_column] = today
            record[self.dimension.expiration_date_column] = None
            record[self.dimension.is_current_column] = True
            
            self.lookup[natural_key] = self._next_sk
            self._next_sk += 1
            self.data.append(record)
            return 1
        
        return 0
    
    def get_surrogate_key(self, natural_key: str) -> Optional[int]:
        """Get surrogate key for a natural key."""
        return self.lookup.get(natural_key)
    
    def get_data(self) -> List[Dict[str, Any]]:
        """Get all dimension data."""
        return self.data


class FactLoader:
    """
    Loads data into fact tables with dimension lookups.
    """
    
    def __init__(
        self,
        fact_table: FactTable,
        dimension_loaders: Dict[str, DimensionLoader],
    ):
        self.fact_table = fact_table
        self.dimension_loaders = dimension_loaders
        self.data: List[Dict[str, Any]] = []
        self._next_sk = 1
    
    def load(
        self,
        records: List[Dict[str, Any]],
        key_mapping: Dict[str, str] = None,
        dimension_key_mapping: Dict[str, Tuple[str, str]] = None,
    ) -> int:
        """
        Load records into fact table.
        
        Args:
            records: Source records
            key_mapping: Map source field names to fact column names
            dimension_key_mapping: Map dimension FK to (loader_name, source_field)
        
        Returns:
            Number of records loaded
        """
        key_mapping = key_mapping or {}
        dimension_key_mapping = dimension_key_mapping or {}
        loaded = 0
        
        for record in records:
            fact_record = {}
            
            # Add surrogate key
            pk_col = next(
                (c.name for c in self.fact_table.columns if c.primary_key),
                None
            )
            if pk_col:
                fact_record[pk_col] = self._next_sk
                self._next_sk += 1
            
            # Map source fields to fact columns
            for col in self.fact_table.columns:
                if col.primary_key:
                    continue
                
                source_field = key_mapping.get(col.name, col.name)
                
                if col.foreign_key:
                    # Look up dimension surrogate key
                    if col.name in dimension_key_mapping:
                        loader_name, source_key_field = dimension_key_mapping[col.name]
                        loader = self.dimension_loaders.get(loader_name)
                        if loader:
                            natural_key = str(record.get(source_key_field, ""))
                            fact_record[col.name] = loader.get_surrogate_key(natural_key)
                else:
                    if source_field in record:
                        fact_record[col.name] = record[source_field]
            
            self.data.append(fact_record)
            loaded += 1
        
        return loaded
    
    def get_data(self) -> List[Dict[str, Any]]:
        """Get all fact data."""
        return self.data


class WarehouseBuilder:
    """
    Builds complete data warehouse from source data.
    """
    
    def __init__(self, output_dir: Path = None):
        self.output_dir = output_dir or Path("data/warehouse")
        self.schemas: Dict[str, StarSchema] = {}
        self.dimension_loaders: Dict[str, Dict[str, DimensionLoader]] = {}
        self.fact_loaders: Dict[str, FactLoader] = {}
        
        self.logger = logging.getLogger(f"{__name__}.WarehouseBuilder")
    
    def register_schema(self, schema: StarSchema):
        """Register a star schema."""
        self.schemas[schema.name] = schema
        
        # Create dimension loaders
        self.dimension_loaders[schema.name] = {}
        for dim_name, dim in schema.dimensions.items():
            self.dimension_loaders[schema.name][dim_name] = DimensionLoader(dim)
        
        self.logger.info(f"Registered schema: {schema.name}")
    
    def build_health_warehouse(self, source_data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Build health domain warehouse."""
        schema = create_health_star_schema()
        self.register_schema(schema)
        
        loaders = self.dimension_loaders[schema.name]
        
        # Load date dimension
        date_records = DateDimension.generate(date(2020, 1, 1), date(2025, 12, 31))
        loaders["dim_date"].load(date_records)
        
        # Load time dimension
        time_records = TimeDimension.generate(grain_minutes=15)
        loaders["dim_time"].load(time_records)
        
        # Load patient dimension
        if "patients" in source_data:
            loaders["dim_patient"].load(
                source_data["patients"],
                key_mapping={"patient_id": "patient_id"}
            )
        
        # Load provider dimension
        if "providers" in source_data:
            loaders["dim_provider"].load(
                source_data["providers"],
                key_mapping={"provider_id": "provider_id"}
            )
        
        # Load encounter type dimension
        encounter_types = [
            {"encounter_type_code": "OUTPATIENT", "encounter_type_name": "Outpatient Visit", "category": "Ambulatory", "is_emergency": False, "is_inpatient": False},
            {"encounter_type_code": "INPATIENT", "encounter_type_name": "Inpatient Admission", "category": "Inpatient", "is_emergency": False, "is_inpatient": True},
            {"encounter_type_code": "EMERGENCY", "encounter_type_name": "Emergency Visit", "category": "Emergency", "is_emergency": True, "is_inpatient": False},
            {"encounter_type_code": "TELEHEALTH", "encounter_type_name": "Telehealth Visit", "category": "Virtual", "is_emergency": False, "is_inpatient": False},
        ]
        loaders["dim_encounter_type"].load(encounter_types)
        
        # Load fact table
        fact_loader = FactLoader(schema.fact_table, loaders)
        if "encounters" in source_data:
            fact_loader.load(
                source_data["encounters"],
                dimension_key_mapping={
                    "patient_sk": ("dim_patient", "patient_id"),
                    "provider_sk": ("dim_provider", "provider_id"),
                }
            )
        
        return self._save_warehouse(schema.name, loaders, fact_loader)
    
    def build_finance_warehouse(self, source_data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Build finance domain warehouse."""
        schema = create_finance_star_schema()
        self.register_schema(schema)
        
        loaders = self.dimension_loaders[schema.name]
        
        # Load date/time dimensions
        date_records = DateDimension.generate(date(2020, 1, 1), date(2025, 12, 31))
        loaders["dim_date"].load(date_records)
        time_records = TimeDimension.generate(grain_minutes=15)
        loaders["dim_time"].load(time_records)
        
        # Load account dimension
        if "accounts" in source_data:
            loaders["dim_account"].load(source_data["accounts"])
        
        # Load customer dimension (derived from accounts)
        if "accounts" in source_data:
            customers = []
            seen = set()
            for acc in source_data["accounts"]:
                cid = acc.get("customer_id")
                if cid and cid not in seen:
                    customers.append({
                        "customer_id": cid,
                        "customer_name": acc.get("customer_name"),
                        "customer_type": acc.get("customer_type"),
                    })
                    seen.add(cid)
            loaders["dim_customer"].load(customers)
        
        # Load merchant dimension (derived from transactions)
        if "transactions" in source_data:
            merchants = []
            seen = set()
            for txn in source_data["transactions"]:
                merchant = txn.get("merchant_name")
                if merchant and merchant not in seen:
                    merchants.append({
                        "merchant_id": merchant.lower().replace(" ", "_"),
                        "merchant_name": merchant,
                        "merchant_category": txn.get("merchant_category"),
                        "merchant_category_code": txn.get("merchant_category_code"),
                    })
                    seen.add(merchant)
            loaders["dim_merchant"].load(merchants)
        
        # Load fact table
        fact_loader = FactLoader(schema.fact_table, loaders)
        if "transactions" in source_data:
            fact_loader.load(
                source_data["transactions"],
                dimension_key_mapping={
                    "account_sk": ("dim_account", "account_id"),
                }
            )
        
        return self._save_warehouse(schema.name, loaders, fact_loader)
    
    def build_tech_warehouse(self, source_data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Build tech domain warehouse."""
        schema = create_tech_star_schema()
        self.register_schema(schema)
        
        loaders = self.dimension_loaders[schema.name]
        
        # Load date/time dimensions
        date_records = DateDimension.generate(date(2020, 1, 1), date(2025, 12, 31))
        loaders["dim_date"].load(date_records)
        time_records = TimeDimension.generate(grain_minutes=15)
        loaders["dim_time"].load(time_records)
        
        # Load server dimension
        if "servers" in source_data:
            loaders["dim_server"].load(source_data["servers"])
        
        # Load application dimension
        if "applications" in source_data:
            loaders["dim_application"].load(source_data["applications"])
        
        # Load fact table
        fact_loader = FactLoader(schema.fact_table, loaders)
        if "metrics" in source_data:
            fact_loader.load(
                source_data["metrics"],
                dimension_key_mapping={
                    "server_sk": ("dim_server", "server_id"),
                }
            )
        
        return self._save_warehouse(schema.name, loaders, fact_loader)
    
    def build_university_warehouse(self, source_data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Build university domain warehouse."""
        schema = create_university_star_schema()
        self.register_schema(schema)
        
        loaders = self.dimension_loaders[schema.name]
        
        # Load date dimension
        date_records = DateDimension.generate(date(2020, 1, 1), date(2025, 12, 31))
        loaders["dim_date"].load(date_records)
        
        # Load student dimension
        if "students" in source_data:
            loaders["dim_student"].load(source_data["students"])
        
        # Load course dimension
        if "courses" in source_data:
            loaders["dim_course"].load(source_data["courses"])
        
        # Load instructor dimension
        if "faculty" in source_data:
            loaders["dim_instructor"].load(
                source_data["faculty"],
                key_mapping={"faculty_id": "faculty_id"}
            )
        
        # Load term dimension
        terms = [
            {"term_code": "FALL_2023", "term_name": "Fall 2023", "academic_year": 2023, "semester": "Fall", "start_date": "2023-08-25", "end_date": "2023-12-15", "is_current": False},
            {"term_code": "SPRING_2024", "term_name": "Spring 2024", "academic_year": 2024, "semester": "Spring", "start_date": "2024-01-15", "end_date": "2024-05-10", "is_current": False},
            {"term_code": "FALL_2024", "term_name": "Fall 2024", "academic_year": 2024, "semester": "Fall", "start_date": "2024-08-25", "end_date": "2024-12-15", "is_current": True},
        ]
        loaders["dim_term"].load(terms)
        
        # Load fact table
        fact_loader = FactLoader(schema.fact_table, loaders)
        if "enrollments" in source_data:
            fact_loader.load(
                source_data["enrollments"],
                dimension_key_mapping={
                    "student_sk": ("dim_student", "student_id"),
                    "course_sk": ("dim_course", "course_id"),
                }
            )
        
        return self._save_warehouse(schema.name, loaders, fact_loader)
    
    def _save_warehouse(
        self,
        schema_name: str,
        loaders: Dict[str, DimensionLoader],
        fact_loader: FactLoader,
    ) -> Dict[str, Any]:
        """Save warehouse data to files."""
        output_path = self.output_dir / schema_name
        output_path.mkdir(parents=True, exist_ok=True)
        
        stats = {"dimensions": {}, "fact": {}}
        
        # Save dimensions
        for dim_name, loader in loaders.items():
            data = loader.get_data()
            file_path = output_path / f"{dim_name}.json"
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2, default=str)
            stats["dimensions"][dim_name] = len(data)
        
        # Save fact table
        fact_data = fact_loader.get_data()
        file_path = output_path / f"{fact_loader.fact_table.name}.json"
        with open(file_path, "w") as f:
            json.dump(fact_data, f, indent=2, default=str)
        stats["fact"]["name"] = fact_loader.fact_table.name
        stats["fact"]["rows"] = len(fact_data)
        
        self.logger.info(f"Saved warehouse: {schema_name}")
        
        return stats
    
    def build_all(self, source_data: Dict[str, Dict[str, List[Dict]]]) -> Dict[str, Any]:
        """Build all domain warehouses."""
        results = {}
        
        if "health" in source_data:
            results["health"] = self.build_health_warehouse(source_data["health"])
        
        if "finance" in source_data:
            results["finance"] = self.build_finance_warehouse(source_data["finance"])
        
        if "tech" in source_data:
            results["tech"] = self.build_tech_warehouse(source_data["tech"])
        
        if "university" in source_data:
            results["university"] = self.build_university_warehouse(source_data["university"])
        
        return results
