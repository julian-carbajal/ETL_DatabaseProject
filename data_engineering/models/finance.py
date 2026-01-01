"""
Finance Domain Models - SOX Compliant Financial Data Models.
Covers: Accounts, transactions, investments, loans, credit scores, financial statements.
"""

from datetime import datetime, date
from typing import Optional
from sqlalchemy import (
    Column, Integer, String, DateTime, Date, Float, Boolean,
    Text, ForeignKey, Numeric, Index, CheckConstraint, UniqueConstraint
)
from sqlalchemy.orm import relationship
import enum

from .base import Base, TimestampMixin, AuditMixin, DataLineageMixin


class AccountType(enum.Enum):
    CHECKING = "CHECKING"
    SAVINGS = "SAVINGS"
    MONEY_MARKET = "MONEY_MARKET"
    CD = "CD"
    IRA = "IRA"
    BROKERAGE = "BROKERAGE"
    CREDIT_CARD = "CREDIT_CARD"
    LOAN = "LOAN"
    MORTGAGE = "MORTGAGE"


class TransactionType(enum.Enum):
    CREDIT = "CREDIT"
    DEBIT = "DEBIT"
    TRANSFER = "TRANSFER"
    FEE = "FEE"
    INTEREST = "INTEREST"
    DIVIDEND = "DIVIDEND"
    REFUND = "REFUND"


class TransactionStatus(enum.Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    REVERSED = "REVERSED"
    CANCELLED = "CANCELLED"


class Account(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """
    Financial account master record.
    Supports multiple account types with SOX compliance tracking.
    """
    __tablename__ = "finance_accounts"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_number = Column(String(20), unique=True, nullable=False, index=True)  # ACC0000000001
    
    # Account Owner
    customer_id = Column(String(20), nullable=False, index=True)
    customer_name = Column(String(200), nullable=False)
    customer_type = Column(String(20), default="INDIVIDUAL")  # INDIVIDUAL, BUSINESS, TRUST
    
    # Account Details
    account_type = Column(String(20), nullable=False)
    account_name = Column(String(200), nullable=True)
    currency = Column(String(3), default="USD")
    
    # Balances
    current_balance = Column(Numeric(18, 4), default=0)
    available_balance = Column(Numeric(18, 4), default=0)
    pending_balance = Column(Numeric(18, 4), default=0)
    
    # Interest
    interest_rate = Column(Numeric(8, 6), nullable=True)
    apy = Column(Numeric(8, 6), nullable=True)
    
    # Limits
    credit_limit = Column(Numeric(18, 4), nullable=True)
    daily_withdrawal_limit = Column(Numeric(18, 4), nullable=True)
    
    # Status
    status = Column(String(20), default="ACTIVE")  # ACTIVE, FROZEN, CLOSED, DORMANT
    opened_date = Column(Date, nullable=False)
    closed_date = Column(Date, nullable=True)
    last_activity_date = Column(DateTime, nullable=True)
    
    # Branch Information
    branch_id = Column(String(20), nullable=True)
    branch_name = Column(String(200), nullable=True)
    
    # Compliance
    is_verified = Column(Boolean, default=False)
    kyc_status = Column(String(20), default="PENDING")  # PENDING, VERIFIED, FAILED
    aml_risk_score = Column(Integer, nullable=True)  # 0-100
    
    # Relationships
    transactions = relationship("Transaction", back_populates="account", lazy="dynamic")
    investments = relationship("Investment", back_populates="account", lazy="dynamic")
    
    __table_args__ = (
        Index("idx_account_customer", "customer_id", "account_type"),
        Index("idx_account_status", "status", "last_activity_date"),
        CheckConstraint("current_balance >= -credit_limit OR credit_limit IS NULL", 
                       name="check_balance_limit"),
    )
    
    def __repr__(self):
        return f"<Account(number={self.account_number}, type={self.account_type})>"


class Transaction(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """
    Financial transaction records with full audit trail.
    Immutable after completion for SOX compliance.
    """
    __tablename__ = "finance_transactions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String(30), unique=True, nullable=False, index=True)  # TXN + timestamp + seq
    
    account_id = Column(Integer, ForeignKey("finance_accounts.id"), nullable=False)
    
    # Transaction Details
    transaction_type = Column(String(20), nullable=False)
    transaction_date = Column(DateTime, nullable=False, index=True)
    posting_date = Column(DateTime, nullable=True)
    
    # Amount
    amount = Column(Numeric(18, 4), nullable=False)
    currency = Column(String(3), default="USD")
    exchange_rate = Column(Numeric(12, 6), default=1.0)
    amount_usd = Column(Numeric(18, 4), nullable=True)  # Normalized to USD
    
    # Running Balance
    balance_before = Column(Numeric(18, 4), nullable=True)
    balance_after = Column(Numeric(18, 4), nullable=True)
    
    # Description
    description = Column(String(500), nullable=True)
    merchant_name = Column(String(200), nullable=True)
    merchant_category = Column(String(100), nullable=True)
    merchant_category_code = Column(String(10), nullable=True)  # MCC
    
    # Reference
    reference_number = Column(String(50), nullable=True)
    check_number = Column(String(20), nullable=True)
    
    # Transfer Details
    transfer_to_account = Column(String(20), nullable=True)
    transfer_from_account = Column(String(20), nullable=True)
    
    # Status
    status = Column(String(20), default="COMPLETED")
    failure_reason = Column(String(500), nullable=True)
    
    # Location
    transaction_location = Column(String(200), nullable=True)
    ip_address = Column(String(50), nullable=True)
    device_id = Column(String(100), nullable=True)
    
    # Fraud Detection
    fraud_score = Column(Integer, nullable=True)  # 0-100
    is_flagged = Column(Boolean, default=False)
    flag_reason = Column(String(500), nullable=True)
    
    # Relationships
    account = relationship("Account", back_populates="transactions")
    
    __table_args__ = (
        Index("idx_txn_account_date", "account_id", "transaction_date"),
        Index("idx_txn_type_date", "transaction_type", "transaction_date"),
        Index("idx_txn_merchant", "merchant_name", "merchant_category"),
        Index("idx_txn_fraud", "is_flagged", "fraud_score"),
    )
    
    def __repr__(self):
        return f"<Transaction(id={self.transaction_id}, amount={self.amount})>"


class Investment(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """Investment holdings and portfolio positions."""
    __tablename__ = "finance_investments"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    investment_id = Column(String(20), unique=True, nullable=False, index=True)
    
    account_id = Column(Integer, ForeignKey("finance_accounts.id"), nullable=False)
    
    # Security Information
    symbol = Column(String(20), nullable=False, index=True)
    security_name = Column(String(200), nullable=True)
    security_type = Column(String(50), nullable=False)  # STOCK, BOND, ETF, MUTUAL_FUND, OPTION
    cusip = Column(String(9), nullable=True)
    isin = Column(String(12), nullable=True)
    
    # Position
    quantity = Column(Numeric(18, 8), nullable=False)
    cost_basis = Column(Numeric(18, 4), nullable=True)
    cost_basis_per_share = Column(Numeric(18, 4), nullable=True)
    
    # Current Value
    current_price = Column(Numeric(18, 4), nullable=True)
    market_value = Column(Numeric(18, 4), nullable=True)
    unrealized_gain_loss = Column(Numeric(18, 4), nullable=True)
    unrealized_gain_loss_pct = Column(Numeric(8, 4), nullable=True)
    
    # Dates
    acquisition_date = Column(Date, nullable=True)
    last_price_update = Column(DateTime, nullable=True)
    
    # Dividends
    dividend_yield = Column(Numeric(8, 4), nullable=True)
    annual_dividend = Column(Numeric(18, 4), nullable=True)
    
    # Tax Lot
    tax_lot_id = Column(String(50), nullable=True)
    is_short_term = Column(Boolean, nullable=True)
    
    # Relationships
    account = relationship("Account", back_populates="investments")
    
    __table_args__ = (
        Index("idx_investment_symbol", "symbol", "account_id"),
        Index("idx_investment_type", "security_type"),
    )
    
    def __repr__(self):
        return f"<Investment(id={self.investment_id}, symbol={self.symbol})>"


class LoanApplication(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """Loan application and underwriting records."""
    __tablename__ = "finance_loan_applications"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    application_id = Column(String(20), unique=True, nullable=False, index=True)
    
    # Applicant Information
    applicant_id = Column(String(20), nullable=False, index=True)
    applicant_name = Column(String(200), nullable=False)
    applicant_ssn_hash = Column(String(256), nullable=True)
    applicant_dob = Column(Date, nullable=True)
    
    # Employment
    employer_name = Column(String(200), nullable=True)
    employment_status = Column(String(50), nullable=True)
    annual_income = Column(Numeric(18, 2), nullable=True)
    years_employed = Column(Integer, nullable=True)
    
    # Loan Details
    loan_type = Column(String(50), nullable=False)  # PERSONAL, AUTO, MORTGAGE, BUSINESS
    loan_purpose = Column(String(200), nullable=True)
    requested_amount = Column(Numeric(18, 2), nullable=False)
    approved_amount = Column(Numeric(18, 2), nullable=True)
    
    # Terms
    requested_term_months = Column(Integer, nullable=True)
    approved_term_months = Column(Integer, nullable=True)
    interest_rate = Column(Numeric(8, 4), nullable=True)
    apr = Column(Numeric(8, 4), nullable=True)
    monthly_payment = Column(Numeric(18, 2), nullable=True)
    
    # Collateral (for secured loans)
    collateral_type = Column(String(100), nullable=True)
    collateral_value = Column(Numeric(18, 2), nullable=True)
    ltv_ratio = Column(Numeric(8, 4), nullable=True)  # Loan-to-Value
    
    # Credit Assessment
    credit_score = Column(Integer, nullable=True)
    debt_to_income_ratio = Column(Numeric(8, 4), nullable=True)
    
    # Status
    status = Column(String(20), default="PENDING")  # PENDING, APPROVED, DENIED, WITHDRAWN
    application_date = Column(DateTime, nullable=False)
    decision_date = Column(DateTime, nullable=True)
    funding_date = Column(Date, nullable=True)
    
    # Underwriting
    underwriter_id = Column(String(20), nullable=True)
    underwriter_notes = Column(Text, nullable=True)
    denial_reason = Column(String(500), nullable=True)
    
    __table_args__ = (
        Index("idx_loan_applicant", "applicant_id", "application_date"),
        Index("idx_loan_status", "status", "loan_type"),
    )
    
    def __repr__(self):
        return f"<LoanApplication(id={self.application_id}, type={self.loan_type})>"


class CreditScore(Base, TimestampMixin, DataLineageMixin):
    """Credit score history and factors."""
    __tablename__ = "finance_credit_scores"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    score_id = Column(String(20), unique=True, nullable=False, index=True)
    
    customer_id = Column(String(20), nullable=False, index=True)
    
    # Score Information
    score = Column(Integer, nullable=False)
    score_model = Column(String(50), nullable=False)  # FICO, VANTAGE
    score_version = Column(String(20), nullable=True)
    bureau = Column(String(50), nullable=True)  # EXPERIAN, EQUIFAX, TRANSUNION
    
    # Score Date
    score_date = Column(Date, nullable=False)
    
    # Score Factors
    payment_history_score = Column(Integer, nullable=True)
    credit_utilization_pct = Column(Numeric(5, 2), nullable=True)
    credit_age_months = Column(Integer, nullable=True)
    total_accounts = Column(Integer, nullable=True)
    hard_inquiries = Column(Integer, nullable=True)
    derogatory_marks = Column(Integer, nullable=True)
    
    # Balances
    total_debt = Column(Numeric(18, 2), nullable=True)
    total_credit_limit = Column(Numeric(18, 2), nullable=True)
    
    # Change
    score_change = Column(Integer, nullable=True)  # Change from previous
    
    __table_args__ = (
        Index("idx_credit_customer_date", "customer_id", "score_date"),
        UniqueConstraint("customer_id", "score_date", "score_model", name="uq_credit_score"),
    )
    
    def __repr__(self):
        return f"<CreditScore(customer={self.customer_id}, score={self.score})>"


class FinancialStatement(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """Company financial statements for analysis."""
    __tablename__ = "finance_statements"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    statement_id = Column(String(20), unique=True, nullable=False, index=True)
    
    # Company Information
    company_id = Column(String(20), nullable=False, index=True)
    company_name = Column(String(200), nullable=False)
    ticker_symbol = Column(String(10), nullable=True, index=True)
    
    # Statement Details
    statement_type = Column(String(50), nullable=False)  # INCOME, BALANCE_SHEET, CASH_FLOW
    fiscal_year = Column(Integer, nullable=False)
    fiscal_quarter = Column(Integer, nullable=True)  # NULL for annual
    period_end_date = Column(Date, nullable=False)
    
    # Income Statement Items
    revenue = Column(Numeric(18, 2), nullable=True)
    cost_of_revenue = Column(Numeric(18, 2), nullable=True)
    gross_profit = Column(Numeric(18, 2), nullable=True)
    operating_expenses = Column(Numeric(18, 2), nullable=True)
    operating_income = Column(Numeric(18, 2), nullable=True)
    net_income = Column(Numeric(18, 2), nullable=True)
    eps = Column(Numeric(10, 4), nullable=True)
    
    # Balance Sheet Items
    total_assets = Column(Numeric(18, 2), nullable=True)
    total_liabilities = Column(Numeric(18, 2), nullable=True)
    total_equity = Column(Numeric(18, 2), nullable=True)
    cash_and_equivalents = Column(Numeric(18, 2), nullable=True)
    total_debt = Column(Numeric(18, 2), nullable=True)
    
    # Cash Flow Items
    operating_cash_flow = Column(Numeric(18, 2), nullable=True)
    investing_cash_flow = Column(Numeric(18, 2), nullable=True)
    financing_cash_flow = Column(Numeric(18, 2), nullable=True)
    free_cash_flow = Column(Numeric(18, 2), nullable=True)
    
    # Ratios
    profit_margin = Column(Numeric(8, 4), nullable=True)
    roe = Column(Numeric(8, 4), nullable=True)
    roa = Column(Numeric(8, 4), nullable=True)
    debt_to_equity = Column(Numeric(8, 4), nullable=True)
    current_ratio = Column(Numeric(8, 4), nullable=True)
    
    # Audit
    auditor = Column(String(200), nullable=True)
    audit_opinion = Column(String(50), nullable=True)
    
    __table_args__ = (
        Index("idx_statement_company", "company_id", "fiscal_year"),
        Index("idx_statement_ticker", "ticker_symbol", "period_end_date"),
        UniqueConstraint("company_id", "statement_type", "fiscal_year", "fiscal_quarter",
                        name="uq_financial_statement"),
    )
    
    def __repr__(self):
        return f"<FinancialStatement(company={self.company_name}, year={self.fiscal_year})>"
