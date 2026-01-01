"""
Tech Domain Models - Infrastructure and Application Monitoring.
Covers: Servers, applications, incidents, deployments, metrics, API endpoints.
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import (
    Column, Integer, String, DateTime, Date, Float, Boolean,
    Text, ForeignKey, Numeric, Index, CheckConstraint, JSON
)
from sqlalchemy.orm import relationship
import enum

from .base import Base, TimestampMixin, AuditMixin, DataLineageMixin


class ServerStatus(enum.Enum):
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    MAINTENANCE = "MAINTENANCE"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


class IncidentSeverity(enum.Enum):
    SEV1 = "SEV1"  # Critical - System down
    SEV2 = "SEV2"  # High - Major feature impacted
    SEV3 = "SEV3"  # Medium - Minor feature impacted
    SEV4 = "SEV4"  # Low - Cosmetic/minor issues


class Server(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """
    Server/infrastructure inventory and status.
    Tracks physical and virtual servers, cloud instances.
    """
    __tablename__ = "tech_servers"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    server_id = Column(String(20), unique=True, nullable=False, index=True)  # SRV-US-0001
    
    # Server Identity
    hostname = Column(String(200), unique=True, nullable=False)
    fqdn = Column(String(500), nullable=True)
    ip_address_private = Column(String(50), nullable=True)
    ip_address_public = Column(String(50), nullable=True)
    mac_address = Column(String(20), nullable=True)
    
    # Environment
    environment = Column(String(20), nullable=False)  # PROD, STAGING, DEV, QA
    datacenter = Column(String(100), nullable=True)
    region = Column(String(50), nullable=True)
    availability_zone = Column(String(50), nullable=True)
    rack_location = Column(String(50), nullable=True)
    
    # Server Type
    server_type = Column(String(50), nullable=False)  # PHYSICAL, VM, CONTAINER, CLOUD
    cloud_provider = Column(String(50), nullable=True)  # AWS, GCP, AZURE
    instance_type = Column(String(50), nullable=True)  # t3.large, n1-standard-4
    
    # Hardware Specs
    cpu_cores = Column(Integer, nullable=True)
    cpu_model = Column(String(200), nullable=True)
    memory_gb = Column(Integer, nullable=True)
    storage_gb = Column(Integer, nullable=True)
    storage_type = Column(String(50), nullable=True)  # SSD, HDD, NVMe
    
    # Operating System
    os_name = Column(String(100), nullable=True)
    os_version = Column(String(50), nullable=True)
    kernel_version = Column(String(100), nullable=True)
    
    # Status
    status = Column(String(20), default="RUNNING")
    last_boot_time = Column(DateTime, nullable=True)
    uptime_seconds = Column(Integer, nullable=True)
    
    # Ownership
    owner_team = Column(String(100), nullable=True)
    cost_center = Column(String(50), nullable=True)
    
    # Tags (JSON for flexibility)
    tags = Column(JSON, nullable=True)
    
    # Lifecycle
    provisioned_date = Column(Date, nullable=True)
    decommission_date = Column(Date, nullable=True)
    
    # Relationships
    applications = relationship("Application", back_populates="server", lazy="dynamic")
    metrics = relationship("MetricLog", back_populates="server", lazy="dynamic")
    incidents = relationship("Incident", back_populates="server", lazy="dynamic")
    
    __table_args__ = (
        Index("idx_server_env", "environment", "status"),
        Index("idx_server_type", "server_type", "cloud_provider"),
        Index("idx_server_owner", "owner_team"),
    )
    
    def __repr__(self):
        return f"<Server(id={self.server_id}, hostname={self.hostname})>"


class Application(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """Application/service registry."""
    __tablename__ = "tech_applications"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    app_id = Column(String(20), unique=True, nullable=False, index=True)
    
    server_id = Column(Integer, ForeignKey("tech_servers.id"), nullable=True)
    
    # Application Identity
    app_name = Column(String(200), nullable=False)
    app_code = Column(String(50), nullable=True)
    version = Column(String(50), nullable=True)
    
    # Type and Stack
    app_type = Column(String(50), nullable=False)  # WEB, API, BATCH, WORKER, DATABASE
    framework = Column(String(100), nullable=True)
    language = Column(String(50), nullable=True)
    runtime_version = Column(String(50), nullable=True)
    
    # Deployment
    environment = Column(String(20), nullable=False)
    deployment_type = Column(String(50), nullable=True)  # CONTAINER, VM, SERVERLESS
    container_image = Column(String(500), nullable=True)
    
    # Network
    port = Column(Integer, nullable=True)
    protocol = Column(String(20), nullable=True)  # HTTP, HTTPS, TCP, gRPC
    health_check_url = Column(String(500), nullable=True)
    
    # Status
    status = Column(String(20), default="RUNNING")
    last_health_check = Column(DateTime, nullable=True)
    health_status = Column(String(20), nullable=True)  # HEALTHY, UNHEALTHY, UNKNOWN
    
    # Performance
    target_response_time_ms = Column(Integer, nullable=True)
    target_availability_pct = Column(Numeric(5, 2), nullable=True)
    
    # Ownership
    owner_team = Column(String(100), nullable=True)
    tech_lead = Column(String(100), nullable=True)
    on_call_rotation = Column(String(100), nullable=True)
    
    # Documentation
    repo_url = Column(String(500), nullable=True)
    docs_url = Column(String(500), nullable=True)
    runbook_url = Column(String(500), nullable=True)
    
    # Dependencies (JSON array)
    dependencies = Column(JSON, nullable=True)
    
    # Criticality
    criticality = Column(String(20), nullable=True)  # CRITICAL, HIGH, MEDIUM, LOW
    
    # Relationships
    server = relationship("Server", back_populates="applications")
    deployments = relationship("Deployment", back_populates="application", lazy="dynamic")
    endpoints = relationship("APIEndpoint", back_populates="application", lazy="dynamic")
    
    __table_args__ = (
        Index("idx_app_name_env", "app_name", "environment"),
        Index("idx_app_status", "status", "health_status"),
        Index("idx_app_owner", "owner_team"),
    )
    
    def __repr__(self):
        return f"<Application(id={self.app_id}, name={self.app_name})>"


class Incident(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """Incident management and tracking."""
    __tablename__ = "tech_incidents"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    incident_id = Column(String(20), unique=True, nullable=False, index=True)  # INC0000001
    
    server_id = Column(Integer, ForeignKey("tech_servers.id"), nullable=True)
    
    # Incident Details
    title = Column(String(500), nullable=False)
    description = Column(Text, nullable=True)
    severity = Column(String(10), nullable=False)  # SEV1, SEV2, SEV3, SEV4
    priority = Column(String(20), nullable=True)
    
    # Classification
    category = Column(String(100), nullable=True)  # INFRASTRUCTURE, APPLICATION, NETWORK, SECURITY
    subcategory = Column(String(100), nullable=True)
    root_cause = Column(Text, nullable=True)
    
    # Impact
    impact_description = Column(Text, nullable=True)
    affected_users = Column(Integer, nullable=True)
    affected_services = Column(JSON, nullable=True)
    revenue_impact = Column(Numeric(18, 2), nullable=True)
    
    # Status
    status = Column(String(20), default="OPEN")  # OPEN, INVESTIGATING, MITIGATED, RESOLVED, CLOSED
    
    # Timeline
    reported_at = Column(DateTime, nullable=False)
    acknowledged_at = Column(DateTime, nullable=True)
    mitigated_at = Column(DateTime, nullable=True)
    resolved_at = Column(DateTime, nullable=True)
    closed_at = Column(DateTime, nullable=True)
    
    # Assignment
    reporter = Column(String(100), nullable=True)
    assignee = Column(String(100), nullable=True)
    assigned_team = Column(String(100), nullable=True)
    
    # Resolution
    resolution_summary = Column(Text, nullable=True)
    resolution_type = Column(String(50), nullable=True)  # FIX, WORKAROUND, NO_ACTION
    
    # Post-Mortem
    postmortem_url = Column(String(500), nullable=True)
    lessons_learned = Column(Text, nullable=True)
    follow_up_actions = Column(JSON, nullable=True)
    
    # Metrics
    time_to_detect_minutes = Column(Integer, nullable=True)
    time_to_mitigate_minutes = Column(Integer, nullable=True)
    time_to_resolve_minutes = Column(Integer, nullable=True)
    
    # Relationships
    server = relationship("Server", back_populates="incidents")
    
    __table_args__ = (
        Index("idx_incident_severity", "severity", "status"),
        Index("idx_incident_date", "reported_at", "status"),
        Index("idx_incident_assignee", "assignee", "status"),
    )
    
    def __repr__(self):
        return f"<Incident(id={self.incident_id}, severity={self.severity})>"
    
    @property
    def mttr_minutes(self) -> Optional[int]:
        """Mean Time To Resolve."""
        if self.resolved_at and self.reported_at:
            return int((self.resolved_at - self.reported_at).total_seconds() / 60)
        return None


class Deployment(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """Deployment history and tracking."""
    __tablename__ = "tech_deployments"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    deployment_id = Column(String(20), unique=True, nullable=False, index=True)
    
    application_id = Column(Integer, ForeignKey("tech_applications.id"), nullable=False)
    
    # Deployment Details
    version = Column(String(50), nullable=False)
    previous_version = Column(String(50), nullable=True)
    environment = Column(String(20), nullable=False)
    
    # Source
    commit_hash = Column(String(50), nullable=True)
    branch = Column(String(100), nullable=True)
    build_number = Column(String(50), nullable=True)
    artifact_url = Column(String(500), nullable=True)
    
    # Deployment Type
    deployment_type = Column(String(50), nullable=True)  # ROLLING, BLUE_GREEN, CANARY
    rollback_version = Column(String(50), nullable=True)
    
    # Timeline
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Integer, nullable=True)
    
    # Status
    status = Column(String(20), default="IN_PROGRESS")  # IN_PROGRESS, SUCCESS, FAILED, ROLLED_BACK
    failure_reason = Column(Text, nullable=True)
    
    # Approval
    requested_by = Column(String(100), nullable=True)
    approved_by = Column(String(100), nullable=True)
    approval_ticket = Column(String(50), nullable=True)
    
    # Change Management
    change_ticket = Column(String(50), nullable=True)
    change_description = Column(Text, nullable=True)
    
    # Metrics
    tests_passed = Column(Integer, nullable=True)
    tests_failed = Column(Integer, nullable=True)
    code_coverage_pct = Column(Numeric(5, 2), nullable=True)
    
    # Relationships
    application = relationship("Application", back_populates="deployments")
    
    __table_args__ = (
        Index("idx_deployment_app", "application_id", "started_at"),
        Index("idx_deployment_status", "status", "environment"),
    )
    
    def __repr__(self):
        return f"<Deployment(id={self.deployment_id}, version={self.version})>"


class MetricLog(Base, DataLineageMixin):
    """Time-series metrics for servers and applications."""
    __tablename__ = "tech_metrics"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    server_id = Column(Integer, ForeignKey("tech_servers.id"), nullable=False)
    
    # Timestamp
    timestamp = Column(DateTime, nullable=False, index=True)
    
    # CPU Metrics
    cpu_usage_pct = Column(Numeric(5, 2), nullable=True)
    cpu_user_pct = Column(Numeric(5, 2), nullable=True)
    cpu_system_pct = Column(Numeric(5, 2), nullable=True)
    cpu_iowait_pct = Column(Numeric(5, 2), nullable=True)
    load_average_1m = Column(Numeric(8, 4), nullable=True)
    load_average_5m = Column(Numeric(8, 4), nullable=True)
    load_average_15m = Column(Numeric(8, 4), nullable=True)
    
    # Memory Metrics
    memory_used_pct = Column(Numeric(5, 2), nullable=True)
    memory_used_gb = Column(Numeric(10, 2), nullable=True)
    memory_available_gb = Column(Numeric(10, 2), nullable=True)
    swap_used_pct = Column(Numeric(5, 2), nullable=True)
    
    # Disk Metrics
    disk_used_pct = Column(Numeric(5, 2), nullable=True)
    disk_read_mb_s = Column(Numeric(10, 2), nullable=True)
    disk_write_mb_s = Column(Numeric(10, 2), nullable=True)
    disk_iops = Column(Integer, nullable=True)
    
    # Network Metrics
    network_in_mb_s = Column(Numeric(10, 2), nullable=True)
    network_out_mb_s = Column(Numeric(10, 2), nullable=True)
    network_packets_in = Column(Integer, nullable=True)
    network_packets_out = Column(Integer, nullable=True)
    network_errors = Column(Integer, nullable=True)
    
    # Process Metrics
    process_count = Column(Integer, nullable=True)
    thread_count = Column(Integer, nullable=True)
    open_files = Column(Integer, nullable=True)
    
    # Relationships
    server = relationship("Server", back_populates="metrics")
    
    __table_args__ = (
        Index("idx_metric_server_time", "server_id", "timestamp"),
    )
    
    def __repr__(self):
        return f"<MetricLog(server={self.server_id}, time={self.timestamp})>"


class APIEndpoint(Base, TimestampMixin, AuditMixin):
    """API endpoint registry and performance tracking."""
    __tablename__ = "tech_api_endpoints"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    endpoint_id = Column(String(20), unique=True, nullable=False, index=True)
    
    application_id = Column(Integer, ForeignKey("tech_applications.id"), nullable=False)
    
    # Endpoint Details
    path = Column(String(500), nullable=False)
    method = Column(String(10), nullable=False)  # GET, POST, PUT, DELETE, PATCH
    version = Column(String(20), nullable=True)
    
    # Description
    name = Column(String(200), nullable=True)
    description = Column(Text, nullable=True)
    
    # Authentication
    auth_required = Column(Boolean, default=True)
    auth_type = Column(String(50), nullable=True)  # API_KEY, OAUTH, JWT, BASIC
    
    # Rate Limiting
    rate_limit_per_minute = Column(Integer, nullable=True)
    rate_limit_per_day = Column(Integer, nullable=True)
    
    # Performance SLAs
    target_latency_p50_ms = Column(Integer, nullable=True)
    target_latency_p95_ms = Column(Integer, nullable=True)
    target_latency_p99_ms = Column(Integer, nullable=True)
    target_availability_pct = Column(Numeric(5, 2), nullable=True)
    
    # Current Performance (rolling averages)
    current_latency_p50_ms = Column(Integer, nullable=True)
    current_latency_p95_ms = Column(Integer, nullable=True)
    current_latency_p99_ms = Column(Integer, nullable=True)
    current_availability_pct = Column(Numeric(5, 2), nullable=True)
    current_error_rate_pct = Column(Numeric(5, 2), nullable=True)
    
    # Traffic
    requests_per_minute = Column(Integer, nullable=True)
    requests_last_24h = Column(Integer, nullable=True)
    
    # Status
    is_deprecated = Column(Boolean, default=False)
    deprecation_date = Column(Date, nullable=True)
    sunset_date = Column(Date, nullable=True)
    
    # Documentation
    docs_url = Column(String(500), nullable=True)
    
    # Relationships
    application = relationship("Application", back_populates="endpoints")
    
    __table_args__ = (
        Index("idx_endpoint_app", "application_id", "method"),
        Index("idx_endpoint_path", "path", "method"),
    )
    
    def __repr__(self):
        return f"<APIEndpoint(id={self.endpoint_id}, path={self.path})>"
