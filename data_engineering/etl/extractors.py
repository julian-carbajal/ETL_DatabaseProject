"""
Data Extractors for ETL Pipeline.
Supports CSV, JSON, API, and Database extraction.
"""

import csv
import json
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterator
from datetime import datetime
import logging

from .base import ETLStep, ETLContext

logger = logging.getLogger(__name__)


class DataExtractor(ETLStep, ABC):
    """Abstract base class for data extractors."""
    
    def __init__(self, name: str, source: str):
        super().__init__(name, f"Extract data from {source}")
        self.source = source
    
    @abstractmethod
    def extract(self, context: ETLContext) -> Any:
        """Extract data from source."""
        pass
    
    def execute(self, context: ETLContext) -> ETLContext:
        """Execute extraction step."""
        start_time = time.time()
        
        context.source_info["source"] = self.source
        context.source_info["extractor"] = self.name
        context.source_info["extraction_started"] = datetime.utcnow().isoformat()
        
        try:
            data = self.extract(context)
            context.raw_data = data
            
            # Update metrics
            if isinstance(data, list):
                context.metrics.records_extracted = len(data)
            elif isinstance(data, dict):
                context.metrics.records_extracted = len(data.get("records", [data]))
            
            context.metrics.extraction_time_seconds = time.time() - start_time
            
            self.logger.info(
                f"Extracted {context.metrics.records_extracted} records "
                f"in {context.metrics.extraction_time_seconds:.2f}s"
            )
            
        except Exception as e:
            context.add_error(e, "extraction")
            raise
        
        return context


class CSVExtractor(DataExtractor):
    """Extract data from CSV files."""
    
    def __init__(
        self,
        name: str,
        file_path: str,
        delimiter: str = ",",
        encoding: str = "utf-8",
        skip_header: bool = False,
        column_mapping: Optional[Dict[str, str]] = None,
    ):
        super().__init__(name, file_path)
        self.file_path = Path(file_path)
        self.delimiter = delimiter
        self.encoding = encoding
        self.skip_header = skip_header
        self.column_mapping = column_mapping or {}
    
    def validate(self, context: ETLContext) -> bool:
        """Validate CSV file exists and is readable."""
        if not self.file_path.exists():
            self.logger.error(f"File not found: {self.file_path}")
            return False
        return True
    
    def extract(self, context: ETLContext) -> List[Dict[str, Any]]:
        """Extract data from CSV file."""
        records = []
        
        with open(self.file_path, "r", encoding=self.encoding) as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            
            for row in reader:
                # Apply column mapping
                if self.column_mapping:
                    row = {
                        self.column_mapping.get(k, k): v
                        for k, v in row.items()
                    }
                records.append(row)
        
        context.source_info["file_path"] = str(self.file_path)
        context.source_info["file_size_bytes"] = self.file_path.stat().st_size
        context.metrics.bytes_processed = self.file_path.stat().st_size
        
        return records


class JSONExtractor(DataExtractor):
    """Extract data from JSON files."""
    
    def __init__(
        self,
        name: str,
        file_path: str,
        records_path: Optional[str] = None,
        encoding: str = "utf-8",
    ):
        super().__init__(name, file_path)
        self.file_path = Path(file_path)
        self.records_path = records_path  # JSONPath-like: "data.records"
        self.encoding = encoding
    
    def validate(self, context: ETLContext) -> bool:
        """Validate JSON file exists."""
        if not self.file_path.exists():
            self.logger.error(f"File not found: {self.file_path}")
            return False
        return True
    
    def extract(self, context: ETLContext) -> List[Dict[str, Any]]:
        """Extract data from JSON file."""
        with open(self.file_path, "r", encoding=self.encoding) as f:
            data = json.load(f)
        
        # Navigate to records path if specified
        if self.records_path:
            for key in self.records_path.split("."):
                data = data[key]
        
        # Ensure we return a list
        if isinstance(data, dict):
            data = [data]
        
        context.source_info["file_path"] = str(self.file_path)
        context.source_info["file_size_bytes"] = self.file_path.stat().st_size
        context.metrics.bytes_processed = self.file_path.stat().st_size
        
        return data


class APIExtractor(DataExtractor):
    """Extract data from REST APIs (simulated for demo)."""
    
    def __init__(
        self,
        name: str,
        endpoint: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        pagination_key: Optional[str] = None,
        max_pages: int = 100,
    ):
        super().__init__(name, endpoint)
        self.endpoint = endpoint
        self.method = method
        self.headers = headers or {}
        self.params = params or {}
        self.pagination_key = pagination_key
        self.max_pages = max_pages
    
    def extract(self, context: ETLContext) -> List[Dict[str, Any]]:
        """
        Extract data from API.
        Note: This is a simulation - in production, use requests library.
        """
        # Simulated API response for demo purposes
        self.logger.info(f"Simulating API call to {self.endpoint}")
        
        context.source_info["endpoint"] = self.endpoint
        context.source_info["method"] = self.method
        
        # Return empty list - actual implementation would use requests
        return []


class DatabaseExtractor(DataExtractor):
    """Extract data from database using SQLAlchemy."""
    
    def __init__(
        self,
        name: str,
        query: str,
        connection_string: str,
        params: Optional[Dict[str, Any]] = None,
        chunk_size: int = 10000,
    ):
        super().__init__(name, "database")
        self.query = query
        self.connection_string = connection_string
        self.params = params or {}
        self.chunk_size = chunk_size
    
    def extract(self, context: ETLContext) -> List[Dict[str, Any]]:
        """Extract data from database."""
        from sqlalchemy import create_engine, text
        
        engine = create_engine(self.connection_string)
        records = []
        
        with engine.connect() as conn:
            result = conn.execute(text(self.query), self.params)
            columns = result.keys()
            
            for row in result:
                records.append(dict(zip(columns, row)))
        
        context.source_info["query"] = self.query[:500]  # Truncate for logging
        
        return records


class IncrementalExtractor(DataExtractor):
    """Extract data incrementally based on watermark."""
    
    def __init__(
        self,
        name: str,
        base_extractor: DataExtractor,
        watermark_column: str,
        watermark_file: str,
    ):
        super().__init__(name, base_extractor.source)
        self.base_extractor = base_extractor
        self.watermark_column = watermark_column
        self.watermark_file = Path(watermark_file)
    
    def get_watermark(self) -> Optional[str]:
        """Get last watermark value."""
        if self.watermark_file.exists():
            return self.watermark_file.read_text().strip()
        return None
    
    def set_watermark(self, value: str):
        """Set new watermark value."""
        self.watermark_file.parent.mkdir(parents=True, exist_ok=True)
        self.watermark_file.write_text(value)
    
    def extract(self, context: ETLContext) -> List[Dict[str, Any]]:
        """Extract data incrementally."""
        watermark = self.get_watermark()
        context.source_info["watermark"] = watermark
        
        # Extract all data
        all_data = self.base_extractor.extract(context)
        
        # Filter based on watermark
        if watermark:
            filtered_data = [
                record for record in all_data
                if str(record.get(self.watermark_column, "")) > watermark
            ]
        else:
            filtered_data = all_data
        
        # Update watermark with max value
        if filtered_data:
            max_watermark = max(
                str(record.get(self.watermark_column, ""))
                for record in filtered_data
            )
            self.set_watermark(max_watermark)
            context.source_info["new_watermark"] = max_watermark
        
        return filtered_data


class StreamingExtractor(DataExtractor):
    """Extract data in streaming fashion for large files."""
    
    def __init__(
        self,
        name: str,
        file_path: str,
        batch_size: int = 1000,
    ):
        super().__init__(name, file_path)
        self.file_path = Path(file_path)
        self.batch_size = batch_size
    
    def extract_batches(self) -> Iterator[List[Dict[str, Any]]]:
        """Yield data in batches."""
        batch = []
        
        with open(self.file_path, "r") as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                batch.append(row)
                
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
            
            if batch:
                yield batch
    
    def extract(self, context: ETLContext) -> List[Dict[str, Any]]:
        """Extract all data (for compatibility)."""
        all_records = []
        for batch in self.extract_batches():
            all_records.extend(batch)
        return all_records
