"""
Pipeline Orchestrator for managing ETL workflows.
Provides scheduling, dependency management, and monitoring.
"""

import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from .base import ETLPipeline, ETLContext, ETLStatus

logger = logging.getLogger(__name__)


class JobState(Enum):
    """Job execution state."""
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"


@dataclass
class JobDefinition:
    """Definition of a scheduled job."""
    job_id: str
    pipeline: ETLPipeline
    schedule: Optional[str] = None  # cron expression or interval
    dependencies: List[str] = field(default_factory=list)
    parameters: Dict[str, Any] = field(default_factory=dict)
    retries: int = 3
    timeout_seconds: int = 3600
    enabled: bool = True
    tags: List[str] = field(default_factory=list)


@dataclass
class JobExecution:
    """Record of a job execution."""
    execution_id: str
    job_id: str
    state: JobState
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    context: Optional[ETLContext] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "job_id": self.job_id,
            "state": self.state.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_seconds": (
                (self.ended_at - self.started_at).total_seconds()
                if self.started_at and self.ended_at else None
            ),
            "error_message": self.error_message,
            "retry_count": self.retry_count,
        }


class PipelineOrchestrator:
    """
    Orchestrates multiple ETL pipelines with dependencies.
    Provides scheduling, monitoring, and execution management.
    """
    
    def __init__(
        self,
        name: str = "ETLOrchestrator",
        max_parallel_jobs: int = 4,
        state_file: Optional[str] = None,
    ):
        self.name = name
        self.max_parallel_jobs = max_parallel_jobs
        self.state_file = Path(state_file) if state_file else None
        
        self.jobs: Dict[str, JobDefinition] = {}
        self.executions: Dict[str, JobExecution] = {}
        self.execution_history: List[JobExecution] = []
        
        self.logger = logging.getLogger(f"{__name__}.{name}")
        self._lock = threading.Lock()
        
        # Callbacks
        self.on_job_start: Optional[Callable[[JobExecution], None]] = None
        self.on_job_complete: Optional[Callable[[JobExecution], None]] = None
        self.on_job_fail: Optional[Callable[[JobExecution, Exception], None]] = None
    
    def register_job(self, job: JobDefinition) -> "PipelineOrchestrator":
        """Register a job with the orchestrator."""
        self.jobs[job.job_id] = job
        self.logger.info(f"Registered job: {job.job_id}")
        return self
    
    def register_pipeline(
        self,
        job_id: str,
        pipeline: ETLPipeline,
        dependencies: List[str] = None,
        **kwargs
    ) -> "PipelineOrchestrator":
        """Convenience method to register a pipeline as a job."""
        job = JobDefinition(
            job_id=job_id,
            pipeline=pipeline,
            dependencies=dependencies or [],
            **kwargs
        )
        return self.register_job(job)
    
    def get_execution_order(self) -> List[List[str]]:
        """
        Determine execution order based on dependencies.
        Returns list of job groups that can run in parallel.
        """
        # Build dependency graph
        remaining = set(self.jobs.keys())
        completed = set()
        order = []
        
        while remaining:
            # Find jobs with all dependencies satisfied
            ready = []
            for job_id in remaining:
                job = self.jobs[job_id]
                if all(dep in completed for dep in job.dependencies):
                    ready.append(job_id)
            
            if not ready:
                # Circular dependency or missing dependency
                raise ValueError(
                    f"Cannot resolve dependencies. Remaining: {remaining}"
                )
            
            order.append(ready)
            completed.update(ready)
            remaining -= set(ready)
        
        return order
    
    def run_job(
        self,
        job_id: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> JobExecution:
        """Run a single job."""
        import uuid
        
        if job_id not in self.jobs:
            raise ValueError(f"Job not found: {job_id}")
        
        job = self.jobs[job_id]
        
        if not job.enabled:
            self.logger.info(f"Job {job_id} is disabled, skipping")
            return JobExecution(
                execution_id=str(uuid.uuid4()),
                job_id=job_id,
                state=JobState.SKIPPED,
            )
        
        execution = JobExecution(
            execution_id=str(uuid.uuid4()),
            job_id=job_id,
            state=JobState.RUNNING,
            started_at=datetime.utcnow(),
        )
        
        with self._lock:
            self.executions[execution.execution_id] = execution
        
        self.logger.info(f"Starting job: {job_id} (execution: {execution.execution_id})")
        
        if self.on_job_start:
            self.on_job_start(execution)
        
        try:
            # Merge parameters
            merged_params = {**job.parameters, **(parameters or {})}
            
            # Create context
            context = ETLContext(
                job_name=f"{job_id}_{execution.execution_id}",
                parameters=merged_params,
            )
            
            # Run pipeline
            context = job.pipeline.run(context)
            
            execution.context = context
            execution.state = (
                JobState.SUCCESS if context.status == ETLStatus.SUCCESS
                else JobState.FAILED
            )
            
            if context.status != ETLStatus.SUCCESS:
                execution.error_message = (
                    context.errors[-1]["error_message"] if context.errors else "Unknown error"
                )
            
        except Exception as e:
            execution.state = JobState.FAILED
            execution.error_message = str(e)
            self.logger.error(f"Job {job_id} failed: {e}")
            
            if self.on_job_fail:
                self.on_job_fail(execution, e)
        
        finally:
            execution.ended_at = datetime.utcnow()
            
            with self._lock:
                self.execution_history.append(execution)
            
            if self.on_job_complete:
                self.on_job_complete(execution)
            
            self._save_state()
        
        return execution
    
    def run_all(
        self,
        parameters: Optional[Dict[str, Any]] = None,
        parallel: bool = True,
    ) -> Dict[str, JobExecution]:
        """
        Run all registered jobs respecting dependencies.
        
        Args:
            parameters: Global parameters to pass to all jobs
            parallel: Whether to run independent jobs in parallel
        
        Returns:
            Dictionary of job_id -> JobExecution
        """
        execution_order = self.get_execution_order()
        results: Dict[str, JobExecution] = {}
        failed_jobs = set()
        
        self.logger.info(f"Starting orchestration with {len(self.jobs)} jobs")
        self.logger.info(f"Execution order: {execution_order}")
        
        for job_group in execution_order:
            # Filter out jobs whose dependencies failed
            runnable = [
                job_id for job_id in job_group
                if not any(dep in failed_jobs for dep in self.jobs[job_id].dependencies)
            ]
            
            skipped = set(job_group) - set(runnable)
            for job_id in skipped:
                self.logger.warning(f"Skipping {job_id} due to failed dependencies")
                results[job_id] = JobExecution(
                    execution_id="skipped",
                    job_id=job_id,
                    state=JobState.SKIPPED,
                    error_message="Dependency failed",
                )
            
            if parallel and len(runnable) > 1:
                # Run jobs in parallel
                with ThreadPoolExecutor(max_workers=self.max_parallel_jobs) as executor:
                    futures = {
                        executor.submit(self.run_job, job_id, parameters): job_id
                        for job_id in runnable
                    }
                    
                    for future in as_completed(futures):
                        job_id = futures[future]
                        try:
                            execution = future.result()
                            results[job_id] = execution
                            
                            if execution.state == JobState.FAILED:
                                failed_jobs.add(job_id)
                        except Exception as e:
                            self.logger.error(f"Job {job_id} raised exception: {e}")
                            failed_jobs.add(job_id)
            else:
                # Run jobs sequentially
                for job_id in runnable:
                    execution = self.run_job(job_id, parameters)
                    results[job_id] = execution
                    
                    if execution.state == JobState.FAILED:
                        failed_jobs.add(job_id)
        
        # Summary
        success_count = sum(1 for e in results.values() if e.state == JobState.SUCCESS)
        failed_count = sum(1 for e in results.values() if e.state == JobState.FAILED)
        skipped_count = sum(1 for e in results.values() if e.state == JobState.SKIPPED)
        
        self.logger.info(
            f"Orchestration complete: {success_count} succeeded, "
            f"{failed_count} failed, {skipped_count} skipped"
        )
        
        return results
    
    def get_job_status(self, job_id: str) -> Optional[JobExecution]:
        """Get the latest execution status for a job."""
        for execution in reversed(self.execution_history):
            if execution.job_id == job_id:
                return execution
        return None
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """Get summary of all executions."""
        if not self.execution_history:
            return {"total_executions": 0}
        
        success = sum(1 for e in self.execution_history if e.state == JobState.SUCCESS)
        failed = sum(1 for e in self.execution_history if e.state == JobState.FAILED)
        
        durations = [
            (e.ended_at - e.started_at).total_seconds()
            for e in self.execution_history
            if e.started_at and e.ended_at
        ]
        
        return {
            "total_executions": len(self.execution_history),
            "success_count": success,
            "failed_count": failed,
            "success_rate": success / len(self.execution_history) if self.execution_history else 0,
            "avg_duration_seconds": sum(durations) / len(durations) if durations else 0,
            "total_duration_seconds": sum(durations),
        }
    
    def _save_state(self):
        """Save orchestrator state to file."""
        if not self.state_file:
            return
        
        state = {
            "name": self.name,
            "saved_at": datetime.utcnow().isoformat(),
            "jobs": list(self.jobs.keys()),
            "recent_executions": [
                e.to_dict() for e in self.execution_history[-100:]
            ],
            "summary": self.get_execution_summary(),
        }
        
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, "w") as f:
            json.dump(state, f, indent=2)
    
    def generate_dag_visualization(self) -> str:
        """Generate a simple text-based DAG visualization."""
        lines = ["Pipeline DAG:", "=" * 40]
        
        execution_order = self.get_execution_order()
        
        for level, job_group in enumerate(execution_order):
            lines.append(f"\nLevel {level}:")
            for job_id in job_group:
                job = self.jobs[job_id]
                deps = f" <- [{', '.join(job.dependencies)}]" if job.dependencies else ""
                status = "✓" if job.enabled else "✗"
                lines.append(f"  {status} {job_id}{deps}")
        
        return "\n".join(lines)


class WorkflowBuilder:
    """Fluent builder for creating ETL workflows."""
    
    def __init__(self, name: str):
        self.orchestrator = PipelineOrchestrator(name=name)
        self._current_job_id: Optional[str] = None
    
    def add_job(
        self,
        job_id: str,
        pipeline: ETLPipeline,
        **kwargs
    ) -> "WorkflowBuilder":
        """Add a job to the workflow."""
        self.orchestrator.register_pipeline(job_id, pipeline, **kwargs)
        self._current_job_id = job_id
        return self
    
    def depends_on(self, *job_ids: str) -> "WorkflowBuilder":
        """Set dependencies for the current job."""
        if self._current_job_id and self._current_job_id in self.orchestrator.jobs:
            self.orchestrator.jobs[self._current_job_id].dependencies = list(job_ids)
        return self
    
    def with_params(self, **params) -> "WorkflowBuilder":
        """Set parameters for the current job."""
        if self._current_job_id and self._current_job_id in self.orchestrator.jobs:
            self.orchestrator.jobs[self._current_job_id].parameters.update(params)
        return self
    
    def on_start(self, callback: Callable[[JobExecution], None]) -> "WorkflowBuilder":
        """Set job start callback."""
        self.orchestrator.on_job_start = callback
        return self
    
    def on_complete(self, callback: Callable[[JobExecution], None]) -> "WorkflowBuilder":
        """Set job complete callback."""
        self.orchestrator.on_job_complete = callback
        return self
    
    def on_fail(self, callback: Callable[[JobExecution, Exception], None]) -> "WorkflowBuilder":
        """Set job failure callback."""
        self.orchestrator.on_job_fail = callback
        return self
    
    def build(self) -> PipelineOrchestrator:
        """Build and return the orchestrator."""
        return self.orchestrator
