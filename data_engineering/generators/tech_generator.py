"""
Tech Domain Data Generator.
Generates realistic infrastructure and application monitoring data.
"""

import random
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
import json

from .base import BaseDataGenerator


class TechDataGenerator(BaseDataGenerator):
    """Generate realistic tech infrastructure data."""
    
    def __init__(self, seed: Optional[int] = None):
        super().__init__(seed)
        
        # Server configurations
        self.cloud_providers = ["AWS", "GCP", "AZURE", "ON_PREM"]
        self.regions = {
            "AWS": ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"],
            "GCP": ["us-central1", "us-east4", "europe-west1", "asia-east1"],
            "AZURE": ["eastus", "westus2", "westeurope", "southeastasia"],
            "ON_PREM": ["dc-east", "dc-west", "dc-central"],
        }
        
        self.instance_types = {
            "AWS": ["t3.micro", "t3.small", "t3.medium", "t3.large", "m5.large", "m5.xlarge", "c5.large", "r5.large"],
            "GCP": ["e2-micro", "e2-small", "e2-medium", "n1-standard-1", "n1-standard-2", "n1-standard-4"],
            "AZURE": ["Standard_B1s", "Standard_B2s", "Standard_D2s_v3", "Standard_D4s_v3"],
            "ON_PREM": ["small", "medium", "large", "xlarge"],
        }
        
        self.os_options = [
            ("Ubuntu", "22.04 LTS"),
            ("Ubuntu", "20.04 LTS"),
            ("Amazon Linux", "2023"),
            ("CentOS", "8"),
            ("Red Hat Enterprise Linux", "8.6"),
            ("Windows Server", "2022"),
            ("Debian", "11"),
        ]
        
        # Application configurations
        self.app_types = ["WEB", "API", "BATCH", "WORKER", "DATABASE", "CACHE", "QUEUE"]
        self.frameworks = {
            "WEB": ["React", "Angular", "Vue.js", "Next.js", "Django", "Rails"],
            "API": ["FastAPI", "Express.js", "Spring Boot", "Flask", "Go Gin", "ASP.NET Core"],
            "BATCH": ["Apache Spark", "Apache Airflow", "Luigi", "Prefect"],
            "WORKER": ["Celery", "Sidekiq", "Bull", "RabbitMQ Consumer"],
            "DATABASE": ["PostgreSQL", "MySQL", "MongoDB", "Redis", "Elasticsearch"],
            "CACHE": ["Redis", "Memcached", "Varnish"],
            "QUEUE": ["RabbitMQ", "Apache Kafka", "Amazon SQS", "Redis Streams"],
        }
        
        self.languages = {
            "React": "JavaScript",
            "Angular": "TypeScript",
            "Vue.js": "JavaScript",
            "Next.js": "TypeScript",
            "Django": "Python",
            "Rails": "Ruby",
            "FastAPI": "Python",
            "Express.js": "JavaScript",
            "Spring Boot": "Java",
            "Flask": "Python",
            "Go Gin": "Go",
            "ASP.NET Core": "C#",
        }
        
        # Incident categories
        self.incident_categories = [
            ("INFRASTRUCTURE", ["Server Down", "Network Issue", "Storage Full", "Memory Exhaustion"]),
            ("APPLICATION", ["Service Unavailable", "High Latency", "Error Rate Spike", "Deployment Failed"]),
            ("NETWORK", ["DNS Resolution", "Load Balancer Issue", "SSL Certificate", "Firewall Block"]),
            ("SECURITY", ["Unauthorized Access", "DDoS Attack", "Vulnerability Detected", "Data Breach"]),
            ("DATABASE", ["Connection Pool Exhausted", "Slow Queries", "Replication Lag", "Deadlock"]),
        ]
        
        # Team names
        self.teams = [
            "Platform Engineering", "Backend Services", "Frontend Team", "Data Engineering",
            "DevOps", "SRE", "Security", "Infrastructure", "API Team", "Mobile Team",
        ]
    
    def generate(self, count: int) -> List[Dict[str, Any]]:
        """Generate server records."""
        return self.generate_servers(count)
    
    def generate_servers(self, count: int) -> List[Dict[str, Any]]:
        """Generate server/infrastructure records."""
        servers = []
        
        for i in range(count):
            cloud_provider = self.weighted_choice(
                self.cloud_providers,
                [0.45, 0.25, 0.20, 0.10]
            )
            region = random.choice(self.regions[cloud_provider])
            instance_type = random.choice(self.instance_types[cloud_provider])
            os_name, os_version = random.choice(self.os_options)
            
            # Generate specs based on instance type
            if "micro" in instance_type or "small" in instance_type or "B1" in instance_type:
                cpu_cores = random.choice([1, 2])
                memory_gb = random.choice([1, 2, 4])
            elif "medium" in instance_type or "B2" in instance_type:
                cpu_cores = random.choice([2, 4])
                memory_gb = random.choice([4, 8])
            else:
                cpu_cores = random.choice([4, 8, 16])
                memory_gb = random.choice([16, 32, 64])
            
            environment = self.weighted_choice(
                ["PROD", "STAGING", "DEV", "QA"],
                [0.4, 0.2, 0.25, 0.15]
            )
            
            hostname = f"{environment.lower()}-{random.choice(['web', 'api', 'db', 'cache', 'worker'])}-{random.randint(1, 99):02d}"
            
            server = {
                "server_id": f"SRV-{region[:2].upper()}-{random.randint(1000, 9999)}",
                "hostname": hostname,
                "fqdn": f"{hostname}.{region}.internal",
                "ip_address_private": f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}",
                "ip_address_public": f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}" if environment == "PROD" else None,
                "environment": environment,
                "datacenter": region,
                "region": region,
                "availability_zone": f"{region}{random.choice(['a', 'b', 'c'])}",
                "server_type": "CLOUD" if cloud_provider != "ON_PREM" else "PHYSICAL",
                "cloud_provider": cloud_provider if cloud_provider != "ON_PREM" else None,
                "instance_type": instance_type,
                "cpu_cores": cpu_cores,
                "memory_gb": memory_gb,
                "storage_gb": random.choice([50, 100, 200, 500, 1000]),
                "storage_type": random.choice(["SSD", "NVMe", "HDD"]),
                "os_name": os_name,
                "os_version": os_version,
                "status": self.weighted_choice(
                    ["RUNNING", "STOPPED", "MAINTENANCE", "DEGRADED"],
                    [0.90, 0.04, 0.04, 0.02]
                ),
                "last_boot_time": self.random_datetime(
                    datetime.now() - timedelta(days=90),
                    datetime.now()
                ).isoformat(),
                "owner_team": random.choice(self.teams),
                "cost_center": f"CC-{random.randint(1000, 9999)}",
                "tags": json.dumps({
                    "environment": environment,
                    "team": random.choice(self.teams),
                    "project": f"project-{random.randint(1, 20)}",
                }),
                "provisioned_date": self.random_date(
                    date(2020, 1, 1),
                    date.today()
                ).isoformat(),
                "created_at": datetime.utcnow().isoformat(),
            }
            servers.append(server)
        
        return servers
    
    def generate_applications(
        self,
        server_ids: List[str],
        count: int
    ) -> List[Dict[str, Any]]:
        """Generate application records."""
        applications = []
        
        for i in range(count):
            app_type = random.choice(self.app_types)
            framework = random.choice(self.frameworks.get(app_type, ["Custom"]))
            language = self.languages.get(framework, "Python")
            
            environment = self.weighted_choice(
                ["PROD", "STAGING", "DEV", "QA"],
                [0.4, 0.2, 0.25, 0.15]
            )
            
            app_name = f"{random.choice(['user', 'order', 'payment', 'inventory', 'notification', 'auth', 'search', 'analytics', 'reporting', 'integration'])}-{app_type.lower()}"
            
            application = {
                "app_id": self.random_id("APP"),
                "server_id": random.choice(server_ids) if server_ids else None,
                "app_name": app_name,
                "app_code": app_name.upper().replace("-", "_"),
                "version": f"{random.randint(1, 5)}.{random.randint(0, 20)}.{random.randint(0, 100)}",
                "app_type": app_type,
                "framework": framework,
                "language": language,
                "runtime_version": f"{random.randint(3, 4)}.{random.randint(8, 12)}" if language == "Python" else f"{random.randint(16, 20)}.{random.randint(0, 9)}",
                "environment": environment,
                "deployment_type": random.choice(["CONTAINER", "VM", "SERVERLESS"]),
                "container_image": f"registry.company.com/{app_name}:v{random.randint(1, 100)}" if random.random() > 0.3 else None,
                "port": random.choice([80, 443, 3000, 5000, 8000, 8080, 8443]),
                "protocol": random.choice(["HTTP", "HTTPS", "gRPC"]),
                "health_check_url": f"/health" if app_type in ["WEB", "API"] else None,
                "status": self.weighted_choice(
                    ["RUNNING", "STOPPED", "DEPLOYING", "FAILED"],
                    [0.88, 0.05, 0.05, 0.02]
                ),
                "health_status": self.weighted_choice(
                    ["HEALTHY", "UNHEALTHY", "UNKNOWN"],
                    [0.92, 0.05, 0.03]
                ),
                "target_response_time_ms": random.choice([100, 200, 500, 1000]),
                "target_availability_pct": random.choice([99.0, 99.5, 99.9, 99.99]),
                "owner_team": random.choice(self.teams),
                "tech_lead": f"{random.choice(self.first_names)} {random.choice(self.last_names)}",
                "criticality": self.weighted_choice(
                    ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                    [0.15, 0.30, 0.40, 0.15]
                ),
                "repo_url": f"https://github.com/company/{app_name}",
                "created_at": datetime.utcnow().isoformat(),
            }
            applications.append(application)
        
        return applications
    
    def generate_incidents(
        self,
        server_ids: List[str],
        count: int
    ) -> List[Dict[str, Any]]:
        """Generate incident records."""
        incidents = []
        
        for i in range(count):
            category, issues = random.choice(self.incident_categories)
            issue = random.choice(issues)
            
            severity = self.weighted_choice(
                ["SEV1", "SEV2", "SEV3", "SEV4"],
                [0.05, 0.15, 0.40, 0.40]
            )
            
            reported_at = self.random_datetime(
                datetime.now() - timedelta(days=180),
                datetime.now()
            )
            
            # Calculate resolution times based on severity
            if severity == "SEV1":
                ttd = random.randint(1, 15)
                ttm = random.randint(5, 60)
                ttr = random.randint(30, 240)
            elif severity == "SEV2":
                ttd = random.randint(5, 30)
                ttm = random.randint(15, 120)
                ttr = random.randint(60, 480)
            else:
                ttd = random.randint(15, 120)
                ttm = random.randint(30, 240)
                ttr = random.randint(120, 1440)
            
            status = self.weighted_choice(
                ["RESOLVED", "CLOSED", "OPEN", "INVESTIGATING"],
                [0.50, 0.35, 0.10, 0.05]
            )
            
            incident = {
                "incident_id": f"INC{reported_at.strftime('%Y%m%d')}{random.randint(100, 999)}",
                "server_id": random.choice(server_ids) if server_ids and random.random() > 0.3 else None,
                "title": f"{issue} - {category}",
                "description": f"Incident detected: {issue} affecting {category.lower()} systems.",
                "severity": severity,
                "priority": severity,
                "category": category,
                "subcategory": issue,
                "impact_description": f"Users experiencing {random.choice(['degraded performance', 'service unavailability', 'intermittent errors', 'slow response times'])}",
                "affected_users": random.randint(10, 100000) if severity in ["SEV1", "SEV2"] else random.randint(1, 1000),
                "status": status,
                "reported_at": reported_at.isoformat(),
                "acknowledged_at": (reported_at + timedelta(minutes=ttd)).isoformat(),
                "mitigated_at": (reported_at + timedelta(minutes=ttm)).isoformat() if status in ["RESOLVED", "CLOSED"] else None,
                "resolved_at": (reported_at + timedelta(minutes=ttr)).isoformat() if status in ["RESOLVED", "CLOSED"] else None,
                "reporter": f"{random.choice(self.first_names)} {random.choice(self.last_names)}",
                "assignee": f"{random.choice(self.first_names)} {random.choice(self.last_names)}",
                "assigned_team": random.choice(self.teams),
                "resolution_summary": f"Issue resolved by {random.choice(['restarting service', 'scaling resources', 'rolling back deployment', 'applying hotfix', 'clearing cache'])}" if status in ["RESOLVED", "CLOSED"] else None,
                "time_to_detect_minutes": ttd,
                "time_to_mitigate_minutes": ttm if status in ["RESOLVED", "CLOSED"] else None,
                "time_to_resolve_minutes": ttr if status in ["RESOLVED", "CLOSED"] else None,
                "created_at": datetime.utcnow().isoformat(),
            }
            incidents.append(incident)
        
        return incidents
    
    def generate_deployments(
        self,
        app_ids: List[str],
        count: int
    ) -> List[Dict[str, Any]]:
        """Generate deployment records."""
        deployments = []
        
        for i in range(count):
            started_at = self.random_datetime(
                datetime.now() - timedelta(days=90),
                datetime.now()
            )
            
            duration = random.randint(60, 1800)  # 1-30 minutes
            status = self.weighted_choice(
                ["SUCCESS", "FAILED", "ROLLED_BACK", "IN_PROGRESS"],
                [0.85, 0.08, 0.05, 0.02]
            )
            
            version = f"{random.randint(1, 5)}.{random.randint(0, 20)}.{random.randint(0, 100)}"
            
            deployment = {
                "deployment_id": self.random_id("DEP"),
                "application_id": random.choice(app_ids) if app_ids else None,
                "version": version,
                "previous_version": f"{random.randint(1, 5)}.{random.randint(0, 20)}.{random.randint(0, 100)}",
                "environment": self.weighted_choice(
                    ["PROD", "STAGING", "DEV", "QA"],
                    [0.3, 0.3, 0.25, 0.15]
                ),
                "commit_hash": ''.join(random.choices('0123456789abcdef', k=40)),
                "branch": random.choice(["main", "master", "develop", f"release/{version}"]),
                "build_number": str(random.randint(1000, 9999)),
                "deployment_type": random.choice(["ROLLING", "BLUE_GREEN", "CANARY"]),
                "started_at": started_at.isoformat(),
                "completed_at": (started_at + timedelta(seconds=duration)).isoformat() if status != "IN_PROGRESS" else None,
                "duration_seconds": duration if status != "IN_PROGRESS" else None,
                "status": status,
                "failure_reason": random.choice([
                    "Health check failed", "Timeout exceeded", "Resource limit reached",
                    "Dependency unavailable", "Configuration error"
                ]) if status == "FAILED" else None,
                "requested_by": f"{random.choice(self.first_names)} {random.choice(self.last_names)}",
                "approved_by": f"{random.choice(self.first_names)} {random.choice(self.last_names)}",
                "tests_passed": random.randint(50, 500),
                "tests_failed": random.randint(0, 5) if status == "SUCCESS" else random.randint(1, 20),
                "code_coverage_pct": round(random.uniform(70, 95), 2),
                "created_at": datetime.utcnow().isoformat(),
            }
            deployments.append(deployment)
        
        return deployments
    
    def generate_metrics(
        self,
        server_ids: List[str],
        count: int
    ) -> List[Dict[str, Any]]:
        """Generate time-series metric records."""
        metrics = []
        
        for i in range(count):
            timestamp = self.random_datetime(
                datetime.now() - timedelta(days=7),
                datetime.now()
            )
            
            # Generate correlated metrics
            cpu_base = random.uniform(10, 70)
            memory_base = random.uniform(30, 80)
            
            metric = {
                "server_id": random.choice(server_ids) if server_ids else None,
                "timestamp": timestamp.isoformat(),
                "cpu_usage_pct": round(cpu_base + random.uniform(-10, 20), 2),
                "cpu_user_pct": round(cpu_base * 0.7 + random.uniform(-5, 10), 2),
                "cpu_system_pct": round(cpu_base * 0.2 + random.uniform(-2, 5), 2),
                "cpu_iowait_pct": round(random.uniform(0, 10), 2),
                "load_average_1m": round(random.uniform(0.1, 4.0), 4),
                "load_average_5m": round(random.uniform(0.1, 3.5), 4),
                "load_average_15m": round(random.uniform(0.1, 3.0), 4),
                "memory_used_pct": round(memory_base + random.uniform(-10, 15), 2),
                "memory_used_gb": round(memory_base * 0.32, 2),
                "memory_available_gb": round((100 - memory_base) * 0.32, 2),
                "swap_used_pct": round(random.uniform(0, 20), 2),
                "disk_used_pct": round(random.uniform(20, 80), 2),
                "disk_read_mb_s": round(random.uniform(0, 100), 2),
                "disk_write_mb_s": round(random.uniform(0, 50), 2),
                "disk_iops": random.randint(100, 10000),
                "network_in_mb_s": round(random.uniform(0, 100), 2),
                "network_out_mb_s": round(random.uniform(0, 50), 2),
                "network_packets_in": random.randint(1000, 100000),
                "network_packets_out": random.randint(1000, 100000),
                "network_errors": random.randint(0, 10),
                "process_count": random.randint(50, 500),
                "thread_count": random.randint(100, 2000),
                "open_files": random.randint(100, 10000),
            }
            metrics.append(metric)
        
        return metrics
    
    def generate_api_endpoints(
        self,
        app_ids: List[str],
        count: int
    ) -> List[Dict[str, Any]]:
        """Generate API endpoint records."""
        endpoints = []
        
        paths = [
            "/api/v1/users", "/api/v1/users/{id}", "/api/v1/orders", "/api/v1/orders/{id}",
            "/api/v1/products", "/api/v1/products/{id}", "/api/v1/auth/login", "/api/v1/auth/logout",
            "/api/v1/search", "/api/v1/analytics", "/api/v1/reports", "/api/v1/notifications",
            "/api/v2/users", "/api/v2/orders", "/health", "/metrics", "/ready",
        ]
        
        for i in range(count):
            path = random.choice(paths)
            method = self.weighted_choice(
                ["GET", "POST", "PUT", "DELETE", "PATCH"],
                [0.50, 0.25, 0.12, 0.08, 0.05]
            )
            
            endpoint = {
                "endpoint_id": self.random_id("EP"),
                "application_id": random.choice(app_ids) if app_ids else None,
                "path": path,
                "method": method,
                "version": "v1" if "v1" in path else "v2" if "v2" in path else None,
                "name": path.split("/")[-1].replace("{id}", "ById").title(),
                "auth_required": path not in ["/health", "/metrics", "/ready"],
                "auth_type": random.choice(["JWT", "API_KEY", "OAUTH"]) if path not in ["/health", "/metrics", "/ready"] else None,
                "rate_limit_per_minute": random.choice([60, 100, 500, 1000]),
                "target_latency_p50_ms": random.choice([50, 100, 200]),
                "target_latency_p95_ms": random.choice([200, 500, 1000]),
                "target_latency_p99_ms": random.choice([500, 1000, 2000]),
                "target_availability_pct": 99.9,
                "current_latency_p50_ms": random.randint(20, 150),
                "current_latency_p95_ms": random.randint(100, 800),
                "current_latency_p99_ms": random.randint(200, 1500),
                "current_availability_pct": round(random.uniform(99.0, 100.0), 2),
                "current_error_rate_pct": round(random.uniform(0, 2), 2),
                "requests_per_minute": random.randint(10, 10000),
                "requests_last_24h": random.randint(10000, 10000000),
                "is_deprecated": random.random() < 0.1,
                "created_at": datetime.utcnow().isoformat(),
            }
            endpoints.append(endpoint)
        
        return endpoints
    
    def generate_complete_dataset(
        self,
        num_servers: int = 50,
        apps_per_server: float = 2.0,
        incidents_per_month: int = 20,
        deployments_per_app: float = 5.0,
        metrics_per_server: int = 100,
        endpoints_per_app: float = 8.0,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Generate a complete tech infrastructure dataset."""
        
        servers = self.generate_servers(num_servers)
        server_ids = [s["server_id"] for s in servers]
        
        num_apps = int(num_servers * apps_per_server)
        applications = self.generate_applications(server_ids, num_apps)
        app_ids = [a["app_id"] for a in applications]
        
        return {
            "servers": servers,
            "applications": applications,
            "incidents": self.generate_incidents(server_ids, incidents_per_month * 6),
            "deployments": self.generate_deployments(app_ids, int(num_apps * deployments_per_app)),
            "metrics": self.generate_metrics(server_ids, num_servers * metrics_per_server),
            "api_endpoints": self.generate_api_endpoints(app_ids, int(num_apps * endpoints_per_app)),
        }
