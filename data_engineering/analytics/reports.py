"""
Report Generation and Dashboard Building.
Provides reporting capabilities for all domains.
"""

import json
from datetime import datetime, date
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@dataclass
class Report:
    """Generated report."""
    name: str
    report_type: str
    generated_at: datetime = field(default_factory=datetime.utcnow)
    parameters: Dict[str, Any] = field(default_factory=dict)
    sections: List[Dict[str, Any]] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "report_type": self.report_type,
            "generated_at": self.generated_at.isoformat(),
            "parameters": self.parameters,
            "sections": self.sections,
            "summary": self.summary,
        }
    
    def to_markdown(self) -> str:
        """Convert report to Markdown format."""
        lines = [
            f"# {self.name}",
            f"*Generated: {self.generated_at.strftime('%Y-%m-%d %H:%M:%S')}*",
            "",
        ]
        
        if self.summary:
            lines.append("## Executive Summary")
            for key, value in self.summary.items():
                lines.append(f"- **{key.replace('_', ' ').title()}**: {value}")
            lines.append("")
        
        for section in self.sections:
            lines.append(f"## {section.get('title', 'Section')}")
            
            if section.get("description"):
                lines.append(section["description"])
                lines.append("")
            
            if section.get("metrics"):
                lines.append("### Metrics")
                for metric, value in section["metrics"].items():
                    lines.append(f"- {metric.replace('_', ' ').title()}: {value}")
                lines.append("")
            
            if section.get("table"):
                table = section["table"]
                if table.get("headers") and table.get("rows"):
                    headers = table["headers"]
                    lines.append("| " + " | ".join(headers) + " |")
                    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
                    for row in table["rows"][:20]:  # Limit rows
                        lines.append("| " + " | ".join(str(v) for v in row) + " |")
                    lines.append("")
            
            if section.get("chart_data"):
                lines.append("### Chart Data")
                lines.append("```json")
                lines.append(json.dumps(section["chart_data"], indent=2, default=str))
                lines.append("```")
                lines.append("")
        
        return "\n".join(lines)
    
    def to_html(self) -> str:
        """Convert report to HTML format."""
        html = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            f"<title>{self.name}</title>",
            "<style>",
            "body { font-family: Arial, sans-serif; margin: 40px; }",
            "h1 { color: #333; }",
            "h2 { color: #666; border-bottom: 1px solid #ddd; padding-bottom: 10px; }",
            "table { border-collapse: collapse; width: 100%; margin: 20px 0; }",
            "th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }",
            "th { background-color: #4CAF50; color: white; }",
            "tr:nth-child(even) { background-color: #f2f2f2; }",
            ".metric { display: inline-block; margin: 10px; padding: 15px; background: #f5f5f5; border-radius: 5px; }",
            ".metric-value { font-size: 24px; font-weight: bold; color: #333; }",
            ".metric-label { font-size: 12px; color: #666; }",
            "</style>",
            "</head>",
            "<body>",
            f"<h1>{self.name}</h1>",
            f"<p><em>Generated: {self.generated_at.strftime('%Y-%m-%d %H:%M:%S')}</em></p>",
        ]
        
        if self.summary:
            html.append("<h2>Executive Summary</h2>")
            html.append("<div class='metrics'>")
            for key, value in self.summary.items():
                html.append(f"<div class='metric'>")
                html.append(f"<div class='metric-value'>{value}</div>")
                html.append(f"<div class='metric-label'>{key.replace('_', ' ').title()}</div>")
                html.append("</div>")
            html.append("</div>")
        
        for section in self.sections:
            html.append(f"<h2>{section.get('title', 'Section')}</h2>")
            
            if section.get("description"):
                html.append(f"<p>{section['description']}</p>")
            
            if section.get("metrics"):
                html.append("<div class='metrics'>")
                for metric, value in section["metrics"].items():
                    html.append(f"<div class='metric'>")
                    html.append(f"<div class='metric-value'>{value}</div>")
                    html.append(f"<div class='metric-label'>{metric.replace('_', ' ').title()}</div>")
                    html.append("</div>")
                html.append("</div>")
            
            if section.get("table"):
                table = section["table"]
                if table.get("headers") and table.get("rows"):
                    html.append("<table>")
                    html.append("<tr>" + "".join(f"<th>{h}</th>" for h in table["headers"]) + "</tr>")
                    for row in table["rows"][:50]:
                        html.append("<tr>" + "".join(f"<td>{v}</td>" for v in row) + "</tr>")
                    html.append("</table>")
        
        html.extend(["</body>", "</html>"])
        return "\n".join(html)
    
    def save(self, path: str, format: str = "json"):
        """Save report to file."""
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        if format == "json":
            with open(file_path, "w") as f:
                json.dump(self.to_dict(), f, indent=2, default=str)
        elif format == "markdown" or format == "md":
            with open(file_path, "w") as f:
                f.write(self.to_markdown())
        elif format == "html":
            with open(file_path, "w") as f:
                f.write(self.to_html())


class ReportGenerator:
    """
    Generate reports for different domains and purposes.
    """
    
    def __init__(self, name: str = "ReportGenerator"):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    def generate_health_report(
        self,
        data: Dict[str, List[Dict]],
        metrics: Dict[str, Any],
    ) -> Report:
        """Generate healthcare analytics report."""
        report = Report(
            name="Healthcare Analytics Report",
            report_type="health",
            parameters={"data_sources": list(data.keys())},
        )
        
        # Summary
        report.summary = {
            "total_patients": metrics.get("total_patients", 0),
            "total_encounters": metrics.get("total_encounters", 0),
            "abnormal_lab_rate": f"{metrics.get('abnormal_lab_rate', 0):.1%}",
            "avg_patient_age": metrics.get("avg_patient_age", "N/A"),
        }
        
        # Patient Demographics Section
        if "patients" in data:
            patients = data["patients"]
            gender_dist = {}
            for p in patients:
                g = p.get("gender", "Unknown")
                gender_dist[g] = gender_dist.get(g, 0) + 1
            
            report.sections.append({
                "title": "Patient Demographics",
                "description": "Overview of patient population demographics.",
                "metrics": {
                    "total_patients": len(patients),
                    "avg_age": metrics.get("avg_patient_age", "N/A"),
                    "international_patients": sum(1 for p in patients if p.get("is_international")),
                },
                "chart_data": {
                    "type": "pie",
                    "title": "Gender Distribution",
                    "data": gender_dist,
                },
            })
        
        # Encounters Section
        if "encounters" in data:
            encounters = data["encounters"]
            report.sections.append({
                "title": "Encounter Analysis",
                "description": "Analysis of patient encounters and visits.",
                "metrics": metrics.get("encounters_by_type", {}),
                "chart_data": {
                    "type": "bar",
                    "title": "Encounters by Type",
                    "data": metrics.get("encounters_by_type", {}),
                },
            })
        
        # Lab Results Section
        if "lab_results" in data:
            labs = data["lab_results"]
            report.sections.append({
                "title": "Laboratory Results",
                "description": "Summary of laboratory test results.",
                "metrics": {
                    "total_tests": len(labs),
                    "abnormal_rate": f"{metrics.get('abnormal_lab_rate', 0):.1%}",
                    "critical_rate": f"{metrics.get('critical_lab_rate', 0):.1%}",
                },
            })
        
        return report
    
    def generate_finance_report(
        self,
        data: Dict[str, List[Dict]],
        metrics: Dict[str, Any],
    ) -> Report:
        """Generate financial analytics report."""
        report = Report(
            name="Financial Analytics Report",
            report_type="finance",
            parameters={"data_sources": list(data.keys())},
        )
        
        # Summary
        report.summary = {
            "total_accounts": metrics.get("total_accounts", 0),
            "total_balance": f"${metrics.get('total_balance', 0):,.2f}",
            "total_transactions": metrics.get("total_transactions", 0),
            "loan_approval_rate": f"{metrics.get('loan_approval_rate', 0):.1%}",
        }
        
        # Account Overview
        if "accounts" in data:
            report.sections.append({
                "title": "Account Overview",
                "description": "Summary of financial accounts.",
                "metrics": {
                    "total_accounts": metrics.get("total_accounts", 0),
                    "avg_balance": f"${metrics.get('avg_balance', 0):,.2f}",
                    "high_risk_accounts": metrics.get("high_risk_accounts", 0),
                },
                "chart_data": {
                    "type": "pie",
                    "title": "Accounts by Type",
                    "data": metrics.get("accounts_by_type", {}),
                },
            })
        
        # Transaction Analysis
        if "transactions" in data:
            report.sections.append({
                "title": "Transaction Analysis",
                "description": "Analysis of financial transactions.",
                "metrics": {
                    "total_volume": f"${metrics.get('total_transaction_volume', 0):,.2f}",
                    "avg_amount": f"${metrics.get('avg_transaction_amount', 0):,.2f}",
                    "flagged_rate": f"{metrics.get('flagged_transaction_rate', 0):.2%}",
                },
            })
        
        # Loan Portfolio
        if "loan_applications" in data:
            loans = data["loan_applications"]
            status_dist = {}
            for l in loans:
                s = l.get("status", "Unknown")
                status_dist[s] = status_dist.get(s, 0) + 1
            
            report.sections.append({
                "title": "Loan Portfolio",
                "description": "Analysis of loan applications.",
                "metrics": {
                    "total_applications": len(loans),
                    "approval_rate": f"{metrics.get('loan_approval_rate', 0):.1%}",
                    "total_requested": f"${metrics.get('total_loan_volume_requested', 0):,.2f}",
                },
                "chart_data": {
                    "type": "pie",
                    "title": "Applications by Status",
                    "data": status_dist,
                },
            })
        
        return report
    
    def generate_tech_report(
        self,
        data: Dict[str, List[Dict]],
        metrics: Dict[str, Any],
    ) -> Report:
        """Generate infrastructure analytics report."""
        report = Report(
            name="Infrastructure Analytics Report",
            report_type="tech",
            parameters={"data_sources": list(data.keys())},
        )
        
        # Summary
        report.summary = {
            "total_servers": metrics.get("total_servers", 0),
            "availability_rate": f"{metrics.get('server_availability_rate', 0):.1%}",
            "total_incidents": metrics.get("total_incidents", 0),
            "deployment_success_rate": f"{metrics.get('deployment_success_rate', 0):.1%}",
        }
        
        # Infrastructure Overview
        if "servers" in data:
            report.sections.append({
                "title": "Infrastructure Overview",
                "description": "Summary of server infrastructure.",
                "metrics": {
                    "total_servers": metrics.get("total_servers", 0),
                    "availability": f"{metrics.get('server_availability_rate', 0):.1%}",
                },
                "chart_data": {
                    "type": "bar",
                    "title": "Servers by Environment",
                    "data": metrics.get("servers_by_environment", {}),
                },
            })
        
        # Incident Analysis
        if "incidents" in data:
            report.sections.append({
                "title": "Incident Analysis",
                "description": "Analysis of system incidents.",
                "metrics": {
                    "total_incidents": metrics.get("total_incidents", 0),
                    "mttr": f"{metrics.get('mttr_minutes', 0):.0f} minutes",
                    "resolution_rate": f"{metrics.get('incident_resolution_rate', 0):.1%}",
                },
                "chart_data": {
                    "type": "bar",
                    "title": "Incidents by Severity",
                    "data": metrics.get("incidents_by_severity", {}),
                },
            })
        
        # Deployment Metrics
        if "deployments" in data:
            report.sections.append({
                "title": "Deployment Metrics",
                "description": "Analysis of application deployments.",
                "metrics": {
                    "total_deployments": metrics.get("total_deployments", 0),
                    "success_rate": f"{metrics.get('deployment_success_rate', 0):.1%}",
                    "avg_duration": f"{metrics.get('avg_deployment_duration_seconds', 0):.0f}s",
                },
            })
        
        # Resource Utilization
        if "metrics" in data:
            report.sections.append({
                "title": "Resource Utilization",
                "description": "Server resource utilization metrics.",
                "metrics": {
                    "avg_cpu": f"{metrics.get('avg_cpu_usage', 0):.1f}%",
                    "max_cpu": f"{metrics.get('max_cpu_usage', 0):.1f}%",
                    "avg_memory": f"{metrics.get('avg_memory_usage', 0):.1f}%",
                    "max_memory": f"{metrics.get('max_memory_usage', 0):.1f}%",
                },
            })
        
        return report
    
    def generate_university_report(
        self,
        data: Dict[str, List[Dict]],
        metrics: Dict[str, Any],
    ) -> Report:
        """Generate academic analytics report."""
        report = Report(
            name="Academic Analytics Report",
            report_type="university",
            parameters={"data_sources": list(data.keys())},
        )
        
        # Summary
        report.summary = {
            "total_students": metrics.get("total_students", 0),
            "total_faculty": metrics.get("total_faculty", 0),
            "avg_gpa": metrics.get("avg_gpa", "N/A"),
            "retention_rate": f"{metrics.get('retention_rate', 0):.1%}",
        }
        
        # Student Overview
        if "students" in data:
            report.sections.append({
                "title": "Student Overview",
                "description": "Summary of student population.",
                "metrics": {
                    "total_students": metrics.get("total_students", 0),
                    "avg_gpa": metrics.get("avg_gpa", "N/A"),
                    "honors_students": metrics.get("honors_students", 0),
                    "retention_rate": f"{metrics.get('retention_rate', 0):.1%}",
                },
                "chart_data": {
                    "type": "bar",
                    "title": "Students by Level",
                    "data": metrics.get("students_by_level", {}),
                },
            })
        
        # Faculty Overview
        if "faculty" in data:
            report.sections.append({
                "title": "Faculty Overview",
                "description": "Summary of faculty members.",
                "metrics": {
                    "total_faculty": metrics.get("total_faculty", 0),
                    "total_publications": metrics.get("total_publications", 0),
                    "avg_publications": metrics.get("avg_publications_per_faculty", 0),
                },
                "chart_data": {
                    "type": "pie",
                    "title": "Faculty by Rank",
                    "data": metrics.get("faculty_by_rank", {}),
                },
            })
        
        # Course Enrollment
        if "courses" in data:
            report.sections.append({
                "title": "Course Enrollment",
                "description": "Analysis of course enrollment.",
                "metrics": {
                    "total_courses": metrics.get("total_courses", 0),
                    "total_enrollment": metrics.get("total_course_enrollment", 0),
                    "avg_fill_rate": f"{metrics.get('avg_course_fill_rate', 0):.1%}",
                },
            })
        
        # Academic Performance
        if "enrollments" in data:
            report.sections.append({
                "title": "Academic Performance",
                "description": "Student academic performance analysis.",
                "metrics": {
                    "total_enrollments": metrics.get("total_enrollments", 0),
                    "pass_rate": f"{metrics.get('pass_rate', 0):.1%}",
                },
                "chart_data": {
                    "type": "bar",
                    "title": "Grade Distribution",
                    "data": metrics.get("grade_distribution", {}),
                },
            })
        
        # Research Grants
        if "research_grants" in data:
            report.sections.append({
                "title": "Research Grants",
                "description": "Summary of research funding.",
                "metrics": {
                    "total_grants": metrics.get("total_grants", 0),
                    "active_grants": metrics.get("active_grants", 0),
                    "total_funding": f"${metrics.get('total_grant_funding', 0):,.2f}",
                },
            })
        
        return report


class DashboardBuilder:
    """
    Build dashboard configurations for visualization.
    """
    
    def __init__(self):
        self.widgets: List[Dict[str, Any]] = []
    
    def add_metric_card(
        self,
        title: str,
        value: Any,
        subtitle: str = "",
        trend: Optional[float] = None,
        color: str = "blue",
    ) -> "DashboardBuilder":
        """Add a metric card widget."""
        self.widgets.append({
            "type": "metric_card",
            "title": title,
            "value": value,
            "subtitle": subtitle,
            "trend": trend,
            "color": color,
        })
        return self
    
    def add_chart(
        self,
        title: str,
        chart_type: str,
        data: Dict[str, Any],
        width: str = "half",
    ) -> "DashboardBuilder":
        """Add a chart widget."""
        self.widgets.append({
            "type": "chart",
            "chart_type": chart_type,  # bar, line, pie, area
            "title": title,
            "data": data,
            "width": width,
        })
        return self
    
    def add_table(
        self,
        title: str,
        headers: List[str],
        rows: List[List[Any]],
        width: str = "full",
    ) -> "DashboardBuilder":
        """Add a table widget."""
        self.widgets.append({
            "type": "table",
            "title": title,
            "headers": headers,
            "rows": rows,
            "width": width,
        })
        return self
    
    def add_alert_list(
        self,
        title: str,
        alerts: List[Dict[str, Any]],
    ) -> "DashboardBuilder":
        """Add an alert list widget."""
        self.widgets.append({
            "type": "alert_list",
            "title": title,
            "alerts": alerts,
        })
        return self
    
    def build(self) -> Dict[str, Any]:
        """Build dashboard configuration."""
        return {
            "generated_at": datetime.utcnow().isoformat(),
            "widgets": self.widgets,
            "layout": self._calculate_layout(),
        }
    
    def _calculate_layout(self) -> List[Dict[str, Any]]:
        """Calculate widget layout."""
        layout = []
        row = []
        row_width = 0
        
        for widget in self.widgets:
            width = widget.get("width", "half")
            widget_width = 1.0 if width == "full" else 0.5
            
            if row_width + widget_width > 1.0:
                layout.append(row)
                row = []
                row_width = 0
            
            row.append(widget)
            row_width += widget_width
        
        if row:
            layout.append(row)
        
        return layout
    
    def to_json(self) -> str:
        """Export dashboard as JSON."""
        return json.dumps(self.build(), indent=2, default=str)
    
    def save(self, path: str):
        """Save dashboard configuration to file."""
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, "w") as f:
            f.write(self.to_json())
