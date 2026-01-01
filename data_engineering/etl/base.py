"""
Base ETL Pipeline Framework.
Provides abstract classes and utilities for building ETL pipelines.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from enum import Enum
import logging
import traceback
import uuid
import time

logger = logging.getLogger(__name__)


class ETLStatus(Enum):
    """ETL job status."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    RETRYING = "RETRYING"


@dataclass
class ETLMetrics:
    """Metrics collected during ETL execution."""
    records_extracted: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    records_rejected: int = 0
    records_duplicates: int = 0
    
    extraction_time_seconds: float = 0.0
    transformation_time_seconds: float = 0.0
    loading_time_seconds: float = 0.0
    total_time_seconds: float = 0.0
    
    bytes_processed: int = 0
    memory_peak_mb: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "records_extracted": self.records_extracted,
            "records_transformed": self.records_transformed,
            "records_loaded": self.records_loaded,
            "records_rejected": self.records_rejected,
            "records_duplicates": self.records_duplicates,
            "extraction_time_seconds": round(self.extraction_time_seconds, 3),
            "transformation_time_seconds": round(self.transformation_time_seconds, 3),
            "loading_time_seconds": round(self.loading_time_seconds, 3),
            "total_time_seconds": round(self.total_time_seconds, 3),
            "bytes_processed": self.bytes_processed,
            "throughput_records_per_second": round(
                self.records_loaded / self.total_time_seconds, 2
            ) if self.total_time_seconds > 0 else 0,
        }


@dataclass
class ETLContext:
    """Context object passed through ETL pipeline stages."""
    job_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    job_name: str = ""
    started_at: datetime = field(default_factory=datetime.utcnow)
    ended_at: Optional[datetime] = None
    status: ETLStatus = ETLStatus.PENDING
    
    # Data containers
    raw_data: Any = None
    transformed_data: Any = None
    loaded_count: int = 0
    
    # Metadata
    source_info: Dict[str, Any] = field(default_factory=dict)
    target_info: Dict[str, Any] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    # Metrics and logging
    metrics: ETLMetrics = field(default_factory=ETLMetrics)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Lineage tracking
    parent_job_id: Optional[str] = None
    child_job_ids: List[str] = field(default_factory=list)
    
    def add_error(self, error: Exception, stage: str, record: Any = None):
        """Add error to context."""
        self.errors.append({
            "stage": stage,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "traceback": traceback.format_exc(),
            "record": str(record)[:500] if record else None,
            "timestamp": datetime.utcnow().isoformat(),
        })
    
    def add_warning(self, message: str):
        """Add warning to context."""
        self.warnings.append(f"[{datetime.utcnow().isoformat()}] {message}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for logging/storage."""
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "status": self.status.value,
            "metrics": self.metrics.to_dict(),
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "source_info": self.source_info,
            "target_info": self.target_info,
        }


class ETLStep(ABC):
    """Abstract base class for ETL steps."""
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    @abstractmethod
    def execute(self, context: ETLContext) -> ETLContext:
        """Execute the ETL step."""
        pass
    
    def validate(self, context: ETLContext) -> bool:
        """Validate step can be executed."""
        return True
    
    def rollback(self, context: ETLContext) -> None:
        """Rollback step in case of failure."""
        pass


class ETLPipeline:
    """
    Main ETL Pipeline class.
    Orchestrates extraction, transformation, and loading steps.
    """
    
    def __init__(
        self,
        name: str,
        description: str = "",
        max_retries: int = 3,
        retry_delay_seconds: int = 5,
    ):
        self.name = name
        self.description = description
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        
        self.steps: List[ETLStep] = []
        self.pre_hooks: List[Callable[[ETLContext], None]] = []
        self.post_hooks: List[Callable[[ETLContext], None]] = []
        self.error_handlers: List[Callable[[ETLContext, Exception], None]] = []
        
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    def add_step(self, step: ETLStep) -> "ETLPipeline":
        """Add a step to the pipeline."""
        self.steps.append(step)
        return self
    
    def add_pre_hook(self, hook: Callable[[ETLContext], None]) -> "ETLPipeline":
        """Add a pre-execution hook."""
        self.pre_hooks.append(hook)
        return self
    
    def add_post_hook(self, hook: Callable[[ETLContext], None]) -> "ETLPipeline":
        """Add a post-execution hook."""
        self.post_hooks.append(hook)
        return self
    
    def add_error_handler(
        self, handler: Callable[[ETLContext, Exception], None]
    ) -> "ETLPipeline":
        """Add an error handler."""
        self.error_handlers.append(handler)
        return self
    
    def run(self, context: Optional[ETLContext] = None, **kwargs) -> ETLContext:
        """
        Execute the ETL pipeline.
        
        Args:
            context: Optional pre-configured context
            **kwargs: Additional parameters to pass to context
        
        Returns:
            ETLContext with results and metrics
        """
        if context is None:
            context = ETLContext(job_name=self.name)
        
        context.parameters.update(kwargs)
        context.status = ETLStatus.RUNNING
        start_time = time.time()
        
        self.logger.info(f"Starting pipeline: {self.name} (job_id: {context.job_id})")
        
        # Execute pre-hooks
        for hook in self.pre_hooks:
            try:
                hook(context)
            except Exception as e:
                self.logger.warning(f"Pre-hook failed: {e}")
        
        # Execute pipeline steps
        completed_steps: List[ETLStep] = []
        try:
            for step in self.steps:
                self.logger.info(f"Executing step: {step.name}")
                step_start = time.time()
                
                # Validate step
                if not step.validate(context):
                    raise ValueError(f"Step validation failed: {step.name}")
                
                # Execute with retry logic
                retries = 0
                while retries <= self.max_retries:
                    try:
                        context = step.execute(context)
                        completed_steps.append(step)
                        break
                    except Exception as e:
                        retries += 1
                        if retries > self.max_retries:
                            raise
                        self.logger.warning(
                            f"Step {step.name} failed, retry {retries}/{self.max_retries}: {e}"
                        )
                        context.status = ETLStatus.RETRYING
                        time.sleep(self.retry_delay_seconds)
                
                step_time = time.time() - step_start
                self.logger.info(f"Step {step.name} completed in {step_time:.2f}s")
            
            context.status = ETLStatus.SUCCESS
            self.logger.info(f"Pipeline {self.name} completed successfully")
            
        except Exception as e:
            context.status = ETLStatus.FAILED
            context.add_error(e, "pipeline")
            self.logger.error(f"Pipeline {self.name} failed: {e}")
            
            # Execute error handlers
            for handler in self.error_handlers:
                try:
                    handler(context, e)
                except Exception as he:
                    self.logger.warning(f"Error handler failed: {he}")
            
            # Rollback completed steps in reverse order
            for step in reversed(completed_steps):
                try:
                    step.rollback(context)
                except Exception as re:
                    self.logger.warning(f"Rollback failed for {step.name}: {re}")
        
        finally:
            context.ended_at = datetime.utcnow()
            context.metrics.total_time_seconds = time.time() - start_time
            
            # Execute post-hooks
            for hook in self.post_hooks:
                try:
                    hook(context)
                except Exception as e:
                    self.logger.warning(f"Post-hook failed: {e}")
        
        return context


class BatchProcessor:
    """Process data in batches for memory efficiency."""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
    
    def process(
        self,
        data: List[Any],
        processor: Callable[[List[Any]], List[Any]],
        on_batch_complete: Optional[Callable[[int, int], None]] = None,
    ) -> List[Any]:
        """
        Process data in batches.
        
        Args:
            data: List of records to process
            processor: Function to apply to each batch
            on_batch_complete: Callback after each batch (batch_num, total_processed)
        
        Returns:
            Processed data
        """
        results = []
        total = len(data)
        
        for i in range(0, total, self.batch_size):
            batch = data[i:i + self.batch_size]
            processed = processor(batch)
            results.extend(processed)
            
            if on_batch_complete:
                on_batch_complete(i // self.batch_size + 1, len(results))
        
        return results
