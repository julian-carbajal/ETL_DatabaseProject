"""
Enterprise Data Engineering Dashboard
=====================================

Interactive Streamlit dashboard for visualizing data across all domains.

Run: streamlit run dashboard/app.py
"""

import streamlit as st
import json
from pathlib import Path
from datetime import datetime
import sys

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from analytics.aggregations import MetricsCalculator
from quality.profiler import DataProfiler, DataQualityScorer
from quality.validators import (
    create_health_validator, create_finance_validator,
    create_tech_validator, create_university_validator
)

# Page config
st.set_page_config(
    page_title="Data Engineering Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .metric-value {
        font-size: 36px;
        font-weight: bold;
    }
    .metric-label {
        font-size: 14px;
        opacity: 0.9;
    }
    .domain-header {
        background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%);
        padding: 10px 20px;
        border-radius: 5px;
        color: white;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data
def load_data():
    """Load all domain data from JSON files."""
    data_dir = Path(__file__).parent.parent / "data" / "raw"
    
    all_data = {}
    domains = ["health", "finance", "tech", "university"]
    
    for domain in domains:
        domain_dir = data_dir / domain
        if domain_dir.exists():
            all_data[domain] = {}
            for json_file in domain_dir.glob("*.json"):
                with open(json_file) as f:
                    all_data[domain][json_file.stem] = json.load(f)
    
    return all_data


@st.cache_data
def calculate_all_metrics(data):
    """Calculate metrics for all domains."""
    calculator = MetricsCalculator()
    
    return {
        "health": calculator.calculate_health_metrics(data.get("health", {})),
        "finance": calculator.calculate_finance_metrics(data.get("finance", {})),
        "tech": calculator.calculate_tech_metrics(data.get("tech", {})),
        "university": calculator.calculate_university_metrics(data.get("university", {})),
    }


def render_metric_card(label, value, delta=None, color="blue"):
    """Render a styled metric card."""
    colors = {
        "blue": "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
        "green": "linear-gradient(135deg, #11998e 0%, #38ef7d 100%)",
        "orange": "linear-gradient(135deg, #f093fb 0%, #f5576c 100%)",
        "purple": "linear-gradient(135deg, #4776E6 0%, #8E54E9 100%)",
    }
    
    delta_html = ""
    if delta is not None:
        arrow = "↑" if delta > 0 else "↓" if delta < 0 else "→"
        delta_html = f'<div style="font-size: 12px; margin-top: 5px;">{arrow} {abs(delta):.1f}%</div>'
    
    st.markdown(f"""
    <div style="background: {colors.get(color, colors['blue'])}; padding: 20px; border-radius: 10px; color: white; text-align: center;">
        <div style="font-size: 32px; font-weight: bold;">{value}</div>
        <div style="font-size: 14px; opacity: 0.9;">{label}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


def render_overview(data, metrics):
    """Render overview dashboard."""
    st.markdown("## 📊 Platform Overview")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_records = sum(
            len(records) 
            for domain_data in data.values() 
            for records in domain_data.values()
        )
        render_metric_card("Total Records", f"{total_records:,}", color="blue")
    
    with col2:
        render_metric_card("Domains", "4", color="green")
    
    with col3:
        entities = sum(len(domain_data) for domain_data in data.values())
        render_metric_card("Entity Types", str(entities), color="orange")
    
    with col4:
        render_metric_card("Data Quality", "A", color="purple")
    
    st.markdown("---")
    
    # Domain summaries
    st.markdown("### Domain Summaries")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🏥 Health")
        health_m = metrics.get("health", {})
        st.metric("Patients", health_m.get("total_patients", 0))
        st.metric("Encounters", health_m.get("total_encounters", 0))
        st.metric("Avg Age", health_m.get("avg_patient_age", "N/A"))
        
        st.markdown("#### 💰 Finance")
        finance_m = metrics.get("finance", {})
        st.metric("Accounts", finance_m.get("total_accounts", 0))
        st.metric("Total Balance", f"${finance_m.get('total_balance', 0):,.0f}")
        st.metric("Transactions", finance_m.get("total_transactions", 0))
    
    with col2:
        st.markdown("#### 🖥️ Tech")
        tech_m = metrics.get("tech", {})
        st.metric("Servers", tech_m.get("total_servers", 0))
        st.metric("Availability", f"{tech_m.get('server_availability_rate', 0):.1%}")
        st.metric("Incidents", tech_m.get("total_incidents", 0))
        
        st.markdown("#### 🎓 University")
        uni_m = metrics.get("university", {})
        st.metric("Students", uni_m.get("total_students", 0))
        st.metric("Faculty", uni_m.get("total_faculty", 0))
        st.metric("Avg GPA", uni_m.get("avg_gpa", "N/A"))


def render_health_dashboard(data, metrics):
    """Render health domain dashboard."""
    st.markdown('<div class="domain-header"><h2>🏥 Healthcare Analytics</h2></div>', unsafe_allow_html=True)
    
    health_data = data.get("health", {})
    health_metrics = metrics.get("health", {})
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_metric_card("Patients", health_metrics.get("total_patients", 0), color="blue")
    with col2:
        render_metric_card("Encounters", health_metrics.get("total_encounters", 0), color="green")
    with col3:
        render_metric_card("Avg Age", health_metrics.get("avg_patient_age", "N/A"), color="orange")
    with col4:
        abnormal = health_metrics.get("abnormal_lab_rate", 0)
        render_metric_card("Abnormal Labs", f"{abnormal:.1%}", color="purple")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Encounters by Type")
        encounters_by_type = health_metrics.get("encounters_by_type", {})
        if encounters_by_type:
            st.bar_chart(encounters_by_type)
    
    with col2:
        st.markdown("### Patient Data Sample")
        patients = health_data.get("patients", [])[:10]
        if patients:
            display_data = [
                {
                    "ID": p.get("patient_id", ""),
                    "Name": f"{p.get('first_name', '')} {p.get('last_name', '')}",
                    "DOB": p.get("date_of_birth", ""),
                    "Blood Type": p.get("blood_type", ""),
                }
                for p in patients
            ]
            st.dataframe(display_data, use_container_width=True)
    
    # Lab results analysis
    st.markdown("### Laboratory Results Analysis")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Lab Tests", len(health_data.get("lab_results", [])))
    with col2:
        st.metric("Abnormal Rate", f"{health_metrics.get('abnormal_lab_rate', 0):.1%}")
    with col3:
        st.metric("Critical Rate", f"{health_metrics.get('critical_lab_rate', 0):.1%}")


def render_finance_dashboard(data, metrics):
    """Render finance domain dashboard."""
    st.markdown('<div class="domain-header"><h2>💰 Financial Analytics</h2></div>', unsafe_allow_html=True)
    
    finance_data = data.get("finance", {})
    finance_metrics = metrics.get("finance", {})
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_metric_card("Accounts", finance_metrics.get("total_accounts", 0), color="blue")
    with col2:
        balance = finance_metrics.get("total_balance", 0)
        render_metric_card("Total Balance", f"${balance/1e6:.1f}M", color="green")
    with col3:
        render_metric_card("Transactions", finance_metrics.get("total_transactions", 0), color="orange")
    with col4:
        approval = finance_metrics.get("loan_approval_rate", 0)
        render_metric_card("Loan Approval", f"{approval:.1%}", color="purple")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Accounts by Type")
        accounts_by_type = finance_metrics.get("accounts_by_type", {})
        if accounts_by_type:
            st.bar_chart(accounts_by_type)
    
    with col2:
        st.markdown("### Transaction Metrics")
        st.metric("Total Volume", f"${finance_metrics.get('total_transaction_volume', 0):,.0f}")
        st.metric("Avg Transaction", f"${finance_metrics.get('avg_transaction_amount', 0):,.2f}")
        st.metric("Flagged Rate", f"{finance_metrics.get('flagged_transaction_rate', 0):.2%}")
    
    # Risk analysis
    st.markdown("### Risk Analysis")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("High Risk Accounts", finance_metrics.get("high_risk_accounts", 0))
        st.metric("Avg AML Risk Score", finance_metrics.get("avg_aml_risk_score", 0))
    with col2:
        st.metric("Loan Applications", len(finance_data.get("loan_applications", [])))
        st.metric("Total Requested", f"${finance_metrics.get('total_loan_volume_requested', 0):,.0f}")


def render_tech_dashboard(data, metrics):
    """Render tech domain dashboard."""
    st.markdown('<div class="domain-header"><h2>🖥️ Infrastructure Analytics</h2></div>', unsafe_allow_html=True)
    
    tech_data = data.get("tech", {})
    tech_metrics = metrics.get("tech", {})
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_metric_card("Servers", tech_metrics.get("total_servers", 0), color="blue")
    with col2:
        avail = tech_metrics.get("server_availability_rate", 0)
        render_metric_card("Availability", f"{avail:.1%}", color="green")
    with col3:
        mttr = tech_metrics.get("mttr_minutes", 0)
        render_metric_card("MTTR", f"{mttr:.0f}m", color="orange")
    with col4:
        deploy = tech_metrics.get("deployment_success_rate", 0)
        render_metric_card("Deploy Success", f"{deploy:.1%}", color="purple")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Servers by Environment")
        servers_by_env = tech_metrics.get("servers_by_environment", {})
        if servers_by_env:
            st.bar_chart(servers_by_env)
    
    with col2:
        st.markdown("### Incidents by Severity")
        incidents_by_sev = tech_metrics.get("incidents_by_severity", {})
        if incidents_by_sev:
            st.bar_chart(incidents_by_sev)
    
    # Resource utilization
    st.markdown("### Resource Utilization")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Avg CPU", f"{tech_metrics.get('avg_cpu_usage', 0):.1f}%")
    with col2:
        st.metric("Max CPU", f"{tech_metrics.get('max_cpu_usage', 0):.1f}%")
    with col3:
        st.metric("Avg Memory", f"{tech_metrics.get('avg_memory_usage', 0):.1f}%")
    with col4:
        st.metric("Max Memory", f"{tech_metrics.get('max_memory_usage', 0):.1f}%")


def render_university_dashboard(data, metrics):
    """Render university domain dashboard."""
    st.markdown('<div class="domain-header"><h2>🎓 Academic Analytics</h2></div>', unsafe_allow_html=True)
    
    uni_data = data.get("university", {})
    uni_metrics = metrics.get("university", {})
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_metric_card("Students", uni_metrics.get("total_students", 0), color="blue")
    with col2:
        render_metric_card("Faculty", uni_metrics.get("total_faculty", 0), color="green")
    with col3:
        render_metric_card("Avg GPA", uni_metrics.get("avg_gpa", "N/A"), color="orange")
    with col4:
        retention = uni_metrics.get("retention_rate", 0)
        render_metric_card("Retention", f"{retention:.1%}", color="purple")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Students by Level")
        students_by_level = uni_metrics.get("students_by_level", {})
        if students_by_level:
            st.bar_chart(students_by_level)
    
    with col2:
        st.markdown("### Grade Distribution")
        grade_dist = uni_metrics.get("grade_distribution", {})
        if grade_dist:
            st.bar_chart(grade_dist)
    
    # Research metrics
    st.markdown("### Research & Grants")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Grants", uni_metrics.get("total_grants", 0))
    with col2:
        st.metric("Active Grants", uni_metrics.get("active_grants", 0))
    with col3:
        funding = uni_metrics.get("total_grant_funding", 0)
        st.metric("Total Funding", f"${funding:,.0f}")
    
    # Faculty metrics
    st.markdown("### Faculty Overview")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Faculty by Rank")
        faculty_by_rank = uni_metrics.get("faculty_by_rank", {})
        if faculty_by_rank:
            st.bar_chart(faculty_by_rank)
    with col2:
        st.metric("Total Publications", uni_metrics.get("total_publications", 0))
        st.metric("Avg Publications/Faculty", uni_metrics.get("avg_publications_per_faculty", 0))


def render_data_quality(data):
    """Render data quality dashboard."""
    st.markdown("## 🔍 Data Quality Dashboard")
    
    profiler = DataProfiler()
    scorer = DataQualityScorer()
    
    validators = {
        "health": ("patients", create_health_validator()),
        "finance": ("accounts", create_finance_validator()),
        "tech": ("servers", create_tech_validator()),
        "university": ("students", create_university_validator()),
    }
    
    results = []
    
    for domain, (entity, validator) in validators.items():
        domain_data = data.get(domain, {}).get(entity, [])
        if domain_data:
            profile = profiler.profile(domain_data, f"{domain}_{entity}")
            report = validator.validate(domain_data)
            score = scorer.score(profile, report)
            
            results.append({
                "Domain": domain.title(),
                "Entity": entity.title(),
                "Records": profile.row_count,
                "Completeness": f"{profile.overall_completeness:.1%}",
                "Validity": f"{report.validity_rate:.1%}",
                "Score": score["overall_score"],
                "Grade": score["grade"],
            })
    
    if results:
        st.dataframe(results, use_container_width=True)
        
        # Quality scores chart
        st.markdown("### Quality Scores by Domain")
        scores = {r["Domain"]: r["Score"] for r in results}
        st.bar_chart(scores)


def main():
    """Main dashboard application."""
    
    # Sidebar
    st.sidebar.title("📊 Data Engineering Platform")
    st.sidebar.markdown("---")
    
    page = st.sidebar.radio(
        "Navigation",
        ["Overview", "Health", "Finance", "Tech", "University", "Data Quality"]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.info(
        "Enterprise Data Engineering Platform spanning Health, Finance, "
        "Tech, and University domains."
    )
    
    # Load data
    try:
        data = load_data()
        metrics = calculate_all_metrics(data)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.info("Run `python main.py` first to generate data.")
        return
    
    # Render selected page
    if page == "Overview":
        render_overview(data, metrics)
    elif page == "Health":
        render_health_dashboard(data, metrics)
    elif page == "Finance":
        render_finance_dashboard(data, metrics)
    elif page == "Tech":
        render_tech_dashboard(data, metrics)
    elif page == "University":
        render_university_dashboard(data, metrics)
    elif page == "Data Quality":
        render_data_quality(data)
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"<div style='text-align: center; color: #666;'>"
        f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        f"</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
