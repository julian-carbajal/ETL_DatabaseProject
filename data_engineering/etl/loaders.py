"""
Data Loaders for ETL Pipeline.
Supports database, file, and streaming destinations.
"""

import json
import csv
import time
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional
import logging

from .base import ETLStep, ETLContext

logger = logging.getLogger(__name__)


class DataLoader(ETLStep, ABC):
    """Abstract base class for data loaders."""
    
    def __init__(self, name: str, destination: str):
        super().__init__(name, f"Load data to {destination}")
        self.destination = destination
    
    @abstractmethod
    def load(self, data: List[Dict[str, Any]], context: ETLContext) -> int:
        """Load data to destination. Returns number of records loaded."""
        pass
    
    def execute(self, context: ETLContext) -> ETLContext:
        """Execute loading step."""
        start_time = time.time()
        
        data = context.transformed_data or context.raw_data
        if data is None:
            raise ValueError("No data to load")
        
        context.target_info["destination"] = self.destination
        context.target_info["loader"] = self.name
        context.target_info["load_started"] = datetime.utcnow().isoformat()
        
        try:
            loaded_count = self.load(data, context)
            context.loaded_count = loaded_count
            context.metrics.records_loaded = loaded_count
            context.metrics.loading_time_seconds = time.time() - start_time
            
            self.logger.info(
                f"Loaded {loaded_count} records "
                f"in {context.metrics.loading_time_seconds:.2f}s"
            )
            
        except Exception as e:
            context.add_error(e, "loading")
            raise
        
        return context


class DatabaseLoader(DataLoader):
    """Load data to database using SQLAlchemy."""
    
    def __init__(
        self,
        name: str,
        connection_string: str,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = "append",  # append, replace, fail
        batch_size: int = 1000,
        column_mapping: Optional[Dict[str, str]] = None,
    ):
        super().__init__(name, f"database:{table_name}")
        self.connection_string = connection_string
        self.table_name = table_name
        self.schema = schema
        self.if_exists = if_exists
        self.batch_size = batch_size
        self.column_mapping = column_mapping or {}
    
    def load(self, data: List[Dict[str, Any]], context: ETLContext) -> int:
        """Load data to database table."""
        from sqlalchemy import create_engine, MetaData, Table, inspect
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert
        
        if not data:
            return 0
        
        engine = create_engine(self.connection_string)
        loaded = 0
        
        # Apply column mapping
        if self.column_mapping:
            data = [
                {self.column_mapping.get(k, k): v for k, v in record.items()}
                for record in data
            ]
        
        # Remove internal ETL columns
        clean_data = []
        for record in data:
            clean_record = {
                k: v for k, v in record.items()
                if not k.startswith("_")
            }
            clean_data.append(clean_record)
        
        # Get table metadata
        metadata = MetaData()
        try:
            table = Table(self.table_name, metadata, autoload_with=engine, schema=self.schema)
            table_columns = {c.name for c in table.columns}
        except Exception:
            # Table doesn't exist, will be created
            table_columns = None
        
        # Filter to only columns that exist in table
        if table_columns:
            clean_data = [
                {k: v for k, v in record.items() if k in table_columns}
                for record in clean_data
            ]
        
        with engine.begin() as conn:
            # Handle if_exists
            if self.if_exists == "replace":
                conn.execute(table.delete())
            
            # Batch insert
            for i in range(0, len(clean_data), self.batch_size):
                batch = clean_data[i:i + self.batch_size]
                if batch:
                    conn.execute(table.insert(), batch)
                    loaded += len(batch)
        
        context.target_info["table_name"] = self.table_name
        context.target_info["records_loaded"] = loaded
        
        return loaded


class FileLoader(DataLoader):
    """Load data to files (CSV, JSON, Parquet)."""
    
    def __init__(
        self,
        name: str,
        file_path: str,
        file_format: str = "csv",  # csv, json, jsonl
        encoding: str = "utf-8",
        append: bool = False,
    ):
        super().__init__(name, file_path)
        self.file_path = Path(file_path)
        self.file_format = file_format
        self.encoding = encoding
        self.append = append
    
    def load(self, data: List[Dict[str, Any]], context: ETLContext) -> int:
        """Load data to file."""
        if not data:
            return 0
        
        # Ensure directory exists
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Remove internal ETL columns
        clean_data = [
            {k: v for k, v in record.items() if not k.startswith("_")}
            for record in data
        ]
        
        if self.file_format == "csv":
            return self._load_csv(clean_data)
        elif self.file_format == "json":
            return self._load_json(clean_data)
        elif self.file_format == "jsonl":
            return self._load_jsonl(clean_data)
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")
    
    def _load_csv(self, data: List[Dict[str, Any]]) -> int:
        """Load data to CSV file."""
        mode = "a" if self.append else "w"
        write_header = not (self.append and self.file_path.exists())
        
        with open(self.file_path, mode, newline="", encoding=self.encoding) as f:
            if data:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                if write_header:
                    writer.writeheader()
                writer.writerows(data)
        
        return len(data)
    
    def _load_json(self, data: List[Dict[str, Any]]) -> int:
        """Load data to JSON file."""
        # Convert datetime objects to strings
        def serialize(obj):
            if isinstance(obj, (datetime,)):
                return obj.isoformat()
            return str(obj)
        
        with open(self.file_path, "w", encoding=self.encoding) as f:
            json.dump(data, f, indent=2, default=serialize)
        
        return len(data)
    
    def _load_jsonl(self, data: List[Dict[str, Any]]) -> int:
        """Load data to JSON Lines file."""
        def serialize(obj):
            if isinstance(obj, (datetime,)):
                return obj.isoformat()
            return str(obj)
        
        mode = "a" if self.append else "w"
        
        with open(self.file_path, mode, encoding=self.encoding) as f:
            for record in data:
                f.write(json.dumps(record, default=serialize) + "\n")
        
        return len(data)


class UpsertLoader(DataLoader):
    """Load data with upsert (insert or update) logic."""
    
    def __init__(
        self,
        name: str,
        connection_string: str,
        table_name: str,
        key_columns: List[str],
        update_columns: Optional[List[str]] = None,
        batch_size: int = 1000,
    ):
        super().__init__(name, f"database:{table_name}")
        self.connection_string = connection_string
        self.table_name = table_name
        self.key_columns = key_columns
        self.update_columns = update_columns
        self.batch_size = batch_size
    
    def load(self, data: List[Dict[str, Any]], context: ETLContext) -> int:
        """Load data with upsert logic."""
        from sqlalchemy import create_engine, text
        
        if not data:
            return 0
        
        engine = create_engine(self.connection_string)
        loaded = 0
        updated = 0
        
        # Clean data
        clean_data = [
            {k: v for k, v in record.items() if not k.startswith("_")}
            for record in data
        ]
        
        with engine.begin() as conn:
            for record in clean_data:
                # Check if record exists
                key_conditions = " AND ".join(
                    f"{col} = :{col}" for col in self.key_columns
                )
                check_query = text(
                    f"SELECT 1 FROM {self.table_name} WHERE {key_conditions}"
                )
                
                key_params = {col: record.get(col) for col in self.key_columns}
                result = conn.execute(check_query, key_params).fetchone()
                
                if result:
                    # Update existing record
                    update_cols = self.update_columns or [
                        k for k in record.keys() if k not in self.key_columns
                    ]
                    set_clause = ", ".join(f"{col} = :{col}" for col in update_cols)
                    update_query = text(
                        f"UPDATE {self.table_name} SET {set_clause} WHERE {key_conditions}"
                    )
                    conn.execute(update_query, record)
                    updated += 1
                else:
                    # Insert new record
                    columns = ", ".join(record.keys())
                    values = ", ".join(f":{k}" for k in record.keys())
                    insert_query = text(
                        f"INSERT INTO {self.table_name} ({columns}) VALUES ({values})"
                    )
                    conn.execute(insert_query, record)
                    loaded += 1
        
        context.target_info["records_inserted"] = loaded
        context.target_info["records_updated"] = updated
        
        return loaded + updated


class SCD2Loader(DataLoader):
    """
    Slowly Changing Dimension Type 2 Loader.
    Maintains history of changes with effective dates.
    """
    
    def __init__(
        self,
        name: str,
        connection_string: str,
        table_name: str,
        business_key_columns: List[str],
        tracked_columns: List[str],
        effective_date_column: str = "effective_date",
        end_date_column: str = "end_date",
        current_flag_column: str = "is_current",
    ):
        super().__init__(name, f"database:{table_name}")
        self.connection_string = connection_string
        self.table_name = table_name
        self.business_key_columns = business_key_columns
        self.tracked_columns = tracked_columns
        self.effective_date_column = effective_date_column
        self.end_date_column = end_date_column
        self.current_flag_column = current_flag_column
    
    def load(self, data: List[Dict[str, Any]], context: ETLContext) -> int:
        """Load data using SCD Type 2 logic."""
        from sqlalchemy import create_engine, text
        
        if not data:
            return 0
        
        engine = create_engine(self.connection_string)
        inserted = 0
        updated = 0
        unchanged = 0
        
        now = datetime.utcnow()
        
        with engine.begin() as conn:
            for record in data:
                # Clean record
                clean_record = {k: v for k, v in record.items() if not k.startswith("_")}
                
                # Find current record
                key_conditions = " AND ".join(
                    f"{col} = :{col}" for col in self.business_key_columns
                )
                current_query = text(
                    f"SELECT * FROM {self.table_name} "
                    f"WHERE {key_conditions} AND {self.current_flag_column} = 1"
                )
                
                key_params = {col: clean_record.get(col) for col in self.business_key_columns}
                current = conn.execute(current_query, key_params).fetchone()
                
                if current:
                    # Check if tracked columns changed
                    current_dict = dict(current._mapping)
                    changed = any(
                        clean_record.get(col) != current_dict.get(col)
                        for col in self.tracked_columns
                    )
                    
                    if changed:
                        # Close current record
                        close_query = text(
                            f"UPDATE {self.table_name} "
                            f"SET {self.end_date_column} = :end_date, "
                            f"{self.current_flag_column} = 0 "
                            f"WHERE {key_conditions} AND {self.current_flag_column} = 1"
                        )
                        conn.execute(close_query, {**key_params, "end_date": now})
                        
                        # Insert new version
                        clean_record[self.effective_date_column] = now
                        clean_record[self.end_date_column] = None
                        clean_record[self.current_flag_column] = True
                        
                        columns = ", ".join(clean_record.keys())
                        values = ", ".join(f":{k}" for k in clean_record.keys())
                        insert_query = text(
                            f"INSERT INTO {self.table_name} ({columns}) VALUES ({values})"
                        )
                        conn.execute(insert_query, clean_record)
                        updated += 1
                    else:
                        unchanged += 1
                else:
                    # Insert new record
                    clean_record[self.effective_date_column] = now
                    clean_record[self.end_date_column] = None
                    clean_record[self.current_flag_column] = True
                    
                    columns = ", ".join(clean_record.keys())
                    values = ", ".join(f":{k}" for k in clean_record.keys())
                    insert_query = text(
                        f"INSERT INTO {self.table_name} ({columns}) VALUES ({values})"
                    )
                    conn.execute(insert_query, clean_record)
                    inserted += 1
        
        context.target_info["records_inserted"] = inserted
        context.target_info["records_updated"] = updated
        context.target_info["records_unchanged"] = unchanged
        
        return inserted + updated


class PartitionedLoader(DataLoader):
    """Load data to partitioned tables/files."""
    
    def __init__(
        self,
        name: str,
        base_path: str,
        partition_columns: List[str],
        file_format: str = "json",
    ):
        super().__init__(name, base_path)
        self.base_path = Path(base_path)
        self.partition_columns = partition_columns
        self.file_format = file_format
    
    def load(self, data: List[Dict[str, Any]], context: ETLContext) -> int:
        """Load data to partitioned structure."""
        if not data:
            return 0
        
        # Group data by partition
        partitions = {}
        for record in data:
            partition_key = tuple(
                f"{col}={record.get(col)}"
                for col in self.partition_columns
            )
            if partition_key not in partitions:
                partitions[partition_key] = []
            partitions[partition_key].append(record)
        
        loaded = 0
        
        for partition_key, partition_data in partitions.items():
            # Build partition path
            partition_path = self.base_path
            for part in partition_key:
                partition_path = partition_path / part
            
            partition_path.mkdir(parents=True, exist_ok=True)
            
            # Write data
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            file_path = partition_path / f"data_{timestamp}.{self.file_format}"
            
            file_loader = FileLoader(
                name=f"partition_{partition_key}",
                file_path=str(file_path),
                file_format=self.file_format,
            )
            loaded += file_loader.load(partition_data, context)
        
        context.target_info["partitions_written"] = len(partitions)
        
        return loaded
