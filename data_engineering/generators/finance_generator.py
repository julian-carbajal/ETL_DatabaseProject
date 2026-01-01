"""
Finance Domain Data Generator.
Generates realistic financial data including accounts, transactions, investments, loans.
"""

import random
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
from decimal import Decimal

from .base import BaseDataGenerator


class FinanceDataGenerator(BaseDataGenerator):
    """Generate realistic financial data."""
    
    def __init__(self, seed: Optional[int] = None):
        super().__init__(seed)
        
        # Account types and their characteristics
        self.account_types = {
            "CHECKING": {"min_balance": -500, "max_balance": 50000, "interest": 0.001},
            "SAVINGS": {"min_balance": 0, "max_balance": 200000, "interest": 0.02},
            "MONEY_MARKET": {"min_balance": 1000, "max_balance": 500000, "interest": 0.03},
            "CD": {"min_balance": 1000, "max_balance": 250000, "interest": 0.04},
            "BROKERAGE": {"min_balance": 0, "max_balance": 2000000, "interest": 0},
            "CREDIT_CARD": {"min_balance": -50000, "max_balance": 0, "interest": 0.18},
        }
        
        # Transaction categories
        self.transaction_categories = {
            "DEBIT": [
                ("Groceries", "5411", ["Walmart", "Kroger", "Whole Foods", "Trader Joe's", "Safeway"]),
                ("Restaurants", "5812", ["McDonald's", "Starbucks", "Chipotle", "Olive Garden", "Local Restaurant"]),
                ("Gas", "5541", ["Shell", "Chevron", "ExxonMobil", "BP", "Costco Gas"]),
                ("Utilities", "4900", ["Electric Company", "Gas Company", "Water Utility", "Internet Provider"]),
                ("Healthcare", "8011", ["CVS Pharmacy", "Walgreens", "Doctor's Office", "Hospital"]),
                ("Shopping", "5311", ["Amazon", "Target", "Best Buy", "Home Depot", "Costco"]),
                ("Entertainment", "7832", ["Netflix", "Spotify", "Movie Theater", "Concert Venue"]),
                ("Travel", "4511", ["United Airlines", "Delta", "Hilton Hotels", "Airbnb"]),
                ("Insurance", "6300", ["State Farm", "Geico", "Progressive", "Allstate"]),
                ("Subscriptions", "5968", ["Gym Membership", "Software Subscription", "Magazine"]),
            ],
            "CREDIT": [
                ("Salary", None, ["Employer Direct Deposit"]),
                ("Interest", None, ["Interest Payment"]),
                ("Refund", None, ["Merchant Refund"]),
                ("Transfer", None, ["Account Transfer"]),
            ],
        }
        
        # Stock symbols and sectors
        self.stocks = [
            ("AAPL", "Apple Inc.", "Technology", 150, 200),
            ("MSFT", "Microsoft Corporation", "Technology", 300, 400),
            ("GOOGL", "Alphabet Inc.", "Technology", 120, 160),
            ("AMZN", "Amazon.com Inc.", "Consumer Cyclical", 130, 180),
            ("NVDA", "NVIDIA Corporation", "Technology", 400, 600),
            ("META", "Meta Platforms Inc.", "Technology", 300, 400),
            ("TSLA", "Tesla Inc.", "Consumer Cyclical", 200, 300),
            ("JPM", "JPMorgan Chase & Co.", "Financial", 140, 180),
            ("V", "Visa Inc.", "Financial", 230, 280),
            ("JNJ", "Johnson & Johnson", "Healthcare", 150, 180),
            ("UNH", "UnitedHealth Group", "Healthcare", 480, 550),
            ("PG", "Procter & Gamble", "Consumer Defensive", 140, 165),
            ("HD", "Home Depot Inc.", "Consumer Cyclical", 300, 380),
            ("MA", "Mastercard Inc.", "Financial", 370, 430),
            ("DIS", "Walt Disney Co.", "Communication", 85, 120),
        ]
        
        # Loan types
        self.loan_types = [
            ("PERSONAL", 1000, 50000, 0.08, 0.24, 12, 60),
            ("AUTO", 5000, 80000, 0.04, 0.12, 36, 72),
            ("MORTGAGE", 100000, 1000000, 0.03, 0.08, 180, 360),
            ("STUDENT", 5000, 200000, 0.04, 0.10, 60, 240),
            ("BUSINESS", 10000, 500000, 0.06, 0.18, 12, 84),
        ]
        
        # Company names for financial statements
        self.companies = [
            ("TECH001", "TechCorp Industries", "TECH"),
            ("TECH002", "DataSoft Solutions", "DATS"),
            ("FIN001", "Global Finance Group", "GFG"),
            ("FIN002", "Capital Investments LLC", "CPIL"),
            ("HEALTH001", "MedTech Innovations", "MDTI"),
            ("RETAIL001", "Consumer Brands Inc.", "CBRN"),
            ("ENERGY001", "GreenPower Corp", "GPWR"),
            ("MFG001", "Industrial Manufacturing Co.", "IMCO"),
        ]
    
    def generate(self, count: int) -> List[Dict[str, Any]]:
        """Generate account records."""
        return self.generate_accounts(count)
    
    def generate_accounts(self, count: int) -> List[Dict[str, Any]]:
        """Generate financial account records."""
        accounts = []
        
        for i in range(count):
            first_name, last_name = self.random_name()
            account_type = random.choice(list(self.account_types.keys()))
            type_config = self.account_types[account_type]
            
            balance = round(random.uniform(
                type_config["min_balance"],
                type_config["max_balance"]
            ), 2)
            
            opened_date = self.random_date(
                date(2010, 1, 1),
                date.today() - timedelta(days=30)
            )
            
            account = {
                "account_number": self.random_id("ACC", 10),
                "customer_id": self.random_id("CUS"),
                "customer_name": f"{first_name} {last_name}",
                "customer_type": self.weighted_choice(
                    ["INDIVIDUAL", "BUSINESS", "TRUST"],
                    [0.85, 0.12, 0.03]
                ),
                "account_type": account_type,
                "account_name": f"{first_name}'s {account_type.replace('_', ' ').title()}",
                "currency": "USD",
                "current_balance": balance,
                "available_balance": balance * random.uniform(0.9, 1.0),
                "pending_balance": round(random.uniform(0, abs(balance) * 0.1), 2),
                "interest_rate": type_config["interest"],
                "apy": type_config["interest"] * 1.02,
                "credit_limit": abs(type_config["min_balance"]) * 2 if account_type == "CREDIT_CARD" else None,
                "status": self.weighted_choice(
                    ["ACTIVE", "FROZEN", "DORMANT"],
                    [0.92, 0.03, 0.05]
                ),
                "opened_date": opened_date.isoformat(),
                "last_activity_date": self.random_datetime(
                    datetime.combine(opened_date, datetime.min.time()),
                    datetime.now()
                ).isoformat(),
                "branch_id": self.random_id("BR", 4),
                "is_verified": True,
                "kyc_status": "VERIFIED",
                "aml_risk_score": random.randint(0, 30),
                "created_at": datetime.utcnow().isoformat(),
            }
            accounts.append(account)
        
        return accounts
    
    def generate_transactions(
        self,
        account_ids: List[str],
        count: int
    ) -> List[Dict[str, Any]]:
        """Generate transaction records."""
        transactions = []
        
        for i in range(count):
            txn_type = self.weighted_choice(
                ["DEBIT", "CREDIT", "TRANSFER", "FEE"],
                [0.55, 0.35, 0.08, 0.02]
            )
            
            # Select category based on type
            if txn_type in ["DEBIT", "CREDIT"]:
                categories = self.transaction_categories.get(txn_type, [])
                if categories:
                    category, mcc, merchants = random.choice(categories)
                    merchant = random.choice(merchants)
                else:
                    category, mcc, merchant = "Other", None, "Unknown Merchant"
            else:
                category, mcc, merchant = txn_type, None, None
            
            # Generate amount based on category
            if category == "Salary":
                amount = round(random.uniform(2000, 15000), 2)
            elif category in ["Mortgage", "Rent"]:
                amount = round(random.uniform(1000, 4000), 2)
            elif category == "Travel":
                amount = round(random.uniform(100, 2000), 2)
            else:
                amount = round(random.uniform(5, 500), 2)
            
            txn_date = self.random_datetime(
                datetime.now() - timedelta(days=365),
                datetime.now()
            )
            
            transaction = {
                "transaction_id": f"TXN{txn_date.strftime('%Y%m%d%H%M%S')}{random.randint(1000, 9999)}",
                "account_id": random.choice(account_ids),
                "transaction_type": txn_type,
                "transaction_date": txn_date.isoformat(),
                "posting_date": (txn_date + timedelta(hours=random.randint(0, 48))).isoformat(),
                "amount": amount if txn_type == "CREDIT" else -amount,
                "currency": "USD",
                "exchange_rate": 1.0,
                "description": f"{merchant or category} - {category}",
                "merchant_name": merchant,
                "merchant_category": category,
                "merchant_category_code": mcc,
                "reference_number": self.random_id("REF", 12),
                "status": self.weighted_choice(
                    ["COMPLETED", "PENDING", "FAILED"],
                    [0.95, 0.04, 0.01]
                ),
                "transaction_location": f"{random.choice(self.cities)[0]}, {random.choice(self.cities)[1]}",
                "fraud_score": random.randint(0, 20),
                "is_flagged": random.random() < 0.02,
                "created_at": datetime.utcnow().isoformat(),
            }
            transactions.append(transaction)
        
        return transactions
    
    def generate_investments(
        self,
        account_ids: List[str],
        count: int
    ) -> List[Dict[str, Any]]:
        """Generate investment/portfolio records."""
        investments = []
        
        for i in range(count):
            stock = random.choice(self.stocks)
            symbol, name, sector, price_low, price_high = stock
            
            quantity = round(random.uniform(1, 500), 4)
            cost_basis_per_share = round(random.uniform(price_low * 0.7, price_high), 2)
            current_price = round(random.uniform(price_low, price_high), 2)
            
            cost_basis = round(quantity * cost_basis_per_share, 2)
            market_value = round(quantity * current_price, 2)
            unrealized_gain = round(market_value - cost_basis, 2)
            
            investment = {
                "investment_id": self.random_id("INV"),
                "account_id": random.choice(account_ids),
                "symbol": symbol,
                "security_name": name,
                "security_type": self.weighted_choice(
                    ["STOCK", "ETF", "MUTUAL_FUND", "BOND"],
                    [0.6, 0.25, 0.1, 0.05]
                ),
                "quantity": quantity,
                "cost_basis": cost_basis,
                "cost_basis_per_share": cost_basis_per_share,
                "current_price": current_price,
                "market_value": market_value,
                "unrealized_gain_loss": unrealized_gain,
                "unrealized_gain_loss_pct": round((unrealized_gain / cost_basis) * 100, 2) if cost_basis else 0,
                "acquisition_date": self.random_date(
                    date(2018, 1, 1),
                    date.today() - timedelta(days=30)
                ).isoformat(),
                "last_price_update": datetime.utcnow().isoformat(),
                "dividend_yield": round(random.uniform(0, 4), 2),
                "is_short_term": random.random() < 0.3,
                "created_at": datetime.utcnow().isoformat(),
            }
            investments.append(investment)
        
        return investments
    
    def generate_loan_applications(self, count: int) -> List[Dict[str, Any]]:
        """Generate loan application records."""
        applications = []
        
        for i in range(count):
            first_name, last_name = self.random_name()
            loan_type = random.choice(self.loan_types)
            type_name, min_amt, max_amt, min_rate, max_rate, min_term, max_term = loan_type
            
            requested_amount = round(random.uniform(min_amt, max_amt), 2)
            credit_score = int(random.gauss(720, 80))
            credit_score = max(300, min(850, credit_score))
            
            # Approval logic based on credit score
            if credit_score >= 740:
                status = self.weighted_choice(["APPROVED", "PENDING"], [0.9, 0.1])
                rate = min_rate + random.uniform(0, 0.02)
            elif credit_score >= 670:
                status = self.weighted_choice(["APPROVED", "PENDING", "DENIED"], [0.7, 0.15, 0.15])
                rate = min_rate + random.uniform(0.02, 0.05)
            else:
                status = self.weighted_choice(["DENIED", "PENDING", "APPROVED"], [0.6, 0.2, 0.2])
                rate = max_rate - random.uniform(0, 0.03)
            
            application_date = self.random_datetime(
                datetime.now() - timedelta(days=180),
                datetime.now()
            )
            
            application = {
                "application_id": self.random_id("LOAN"),
                "applicant_id": self.random_id("APP"),
                "applicant_name": f"{first_name} {last_name}",
                "applicant_ssn_hash": self.hash_value(self.random_ssn()),
                "applicant_dob": self.random_date(date(1960, 1, 1), date(2000, 1, 1)).isoformat(),
                "employer_name": random.choice([
                    "Tech Corp", "Finance Inc", "Healthcare Systems", "Retail Group",
                    "Manufacturing Co", "Consulting LLC", "Government Agency",
                ]),
                "employment_status": self.weighted_choice(
                    ["EMPLOYED", "SELF_EMPLOYED", "RETIRED", "UNEMPLOYED"],
                    [0.75, 0.15, 0.07, 0.03]
                ),
                "annual_income": round(random.uniform(30000, 250000), 2),
                "years_employed": random.randint(0, 30),
                "loan_type": type_name,
                "loan_purpose": random.choice([
                    "Debt Consolidation", "Home Improvement", "Major Purchase",
                    "Vehicle Purchase", "Education", "Business Expansion",
                ]),
                "requested_amount": requested_amount,
                "approved_amount": requested_amount * random.uniform(0.8, 1.0) if status == "APPROVED" else None,
                "requested_term_months": random.randint(min_term, max_term),
                "interest_rate": round(rate, 4) if status == "APPROVED" else None,
                "credit_score": credit_score,
                "debt_to_income_ratio": round(random.uniform(0.1, 0.5), 4),
                "status": status,
                "application_date": application_date.isoformat(),
                "decision_date": (
                    (application_date + timedelta(days=random.randint(1, 14))).isoformat()
                    if status != "PENDING" else None
                ),
                "denial_reason": (
                    random.choice([
                        "Insufficient credit history", "High debt-to-income ratio",
                        "Employment verification failed", "Insufficient income",
                    ]) if status == "DENIED" else None
                ),
                "created_at": datetime.utcnow().isoformat(),
            }
            applications.append(application)
        
        return applications
    
    def generate_credit_scores(
        self,
        customer_ids: List[str],
        count: int
    ) -> List[Dict[str, Any]]:
        """Generate credit score history records."""
        scores = []
        bureaus = ["EXPERIAN", "EQUIFAX", "TRANSUNION"]
        
        for i in range(count):
            base_score = int(random.gauss(720, 80))
            base_score = max(300, min(850, base_score))
            
            score_date = self.random_date(
                date.today() - timedelta(days=365),
                date.today()
            )
            
            credit_score = {
                "score_id": self.random_id("CS"),
                "customer_id": random.choice(customer_ids),
                "score": base_score + random.randint(-20, 20),
                "score_model": self.weighted_choice(["FICO", "VANTAGE"], [0.7, 0.3]),
                "score_version": "9.0",
                "bureau": random.choice(bureaus),
                "score_date": score_date.isoformat(),
                "payment_history_score": random.randint(60, 100),
                "credit_utilization_pct": round(random.uniform(5, 80), 2),
                "credit_age_months": random.randint(12, 300),
                "total_accounts": random.randint(3, 25),
                "hard_inquiries": random.randint(0, 5),
                "derogatory_marks": random.randint(0, 3),
                "total_debt": round(random.uniform(1000, 500000), 2),
                "total_credit_limit": round(random.uniform(5000, 200000), 2),
                "score_change": random.randint(-30, 30),
                "created_at": datetime.utcnow().isoformat(),
            }
            scores.append(credit_score)
        
        return scores
    
    def _get_period_end_date(self, year: int, quarter: Optional[int]) -> date:
        """Get the last day of a fiscal period."""
        if quarter is None:
            return date(year, 12, 31)
        
        month = quarter * 3
        if month in [3, 12]:
            day = 31
        elif month in [6, 9]:
            day = 30
        else:
            day = 28
        return date(year, month, day)
    
    def generate_financial_statements(self, count: int) -> List[Dict[str, Any]]:
        """Generate company financial statement records."""
        statements = []
        
        for i in range(count):
            company = random.choice(self.companies)
            company_id, company_name, ticker = company
            
            fiscal_year = random.randint(2019, 2024)
            fiscal_quarter = random.choice([1, 2, 3, 4, None])
            
            # Generate realistic financial metrics
            revenue = round(random.uniform(100000000, 10000000000), 2)
            gross_margin = random.uniform(0.3, 0.7)
            operating_margin = random.uniform(0.1, 0.4)
            net_margin = random.uniform(0.05, 0.25)
            
            statement = {
                "statement_id": self.random_id("FS"),
                "company_id": company_id,
                "company_name": company_name,
                "ticker_symbol": ticker,
                "statement_type": random.choice(["INCOME", "BALANCE_SHEET", "CASH_FLOW"]),
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter,
                "period_end_date": self._get_period_end_date(fiscal_year, fiscal_quarter).isoformat(),
                "revenue": revenue,
                "cost_of_revenue": round(revenue * (1 - gross_margin), 2),
                "gross_profit": round(revenue * gross_margin, 2),
                "operating_expenses": round(revenue * (gross_margin - operating_margin), 2),
                "operating_income": round(revenue * operating_margin, 2),
                "net_income": round(revenue * net_margin, 2),
                "eps": round(revenue * net_margin / random.uniform(100000000, 500000000), 4),
                "total_assets": round(revenue * random.uniform(1.5, 3), 2),
                "total_liabilities": round(revenue * random.uniform(0.5, 1.5), 2),
                "total_equity": round(revenue * random.uniform(0.5, 1.5), 2),
                "operating_cash_flow": round(revenue * random.uniform(0.1, 0.3), 2),
                "free_cash_flow": round(revenue * random.uniform(0.05, 0.2), 2),
                "profit_margin": round(net_margin, 4),
                "roe": round(random.uniform(0.08, 0.25), 4),
                "roa": round(random.uniform(0.04, 0.15), 4),
                "debt_to_equity": round(random.uniform(0.2, 2.0), 4),
                "current_ratio": round(random.uniform(1.0, 3.0), 4),
                "auditor": random.choice([
                    "Deloitte", "PwC", "EY", "KPMG", "BDO", "Grant Thornton"
                ]),
                "audit_opinion": self.weighted_choice(
                    ["UNQUALIFIED", "QUALIFIED", "ADVERSE"],
                    [0.95, 0.04, 0.01]
                ),
                "created_at": datetime.utcnow().isoformat(),
            }
            statements.append(statement)
        
        return statements
    
    def generate_complete_dataset(
        self,
        num_accounts: int = 100,
        transactions_per_account: float = 50.0,
        investments_per_account: float = 5.0,
        num_loan_applications: int = 50,
        credit_scores_per_customer: float = 3.0,
        num_financial_statements: int = 100,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Generate a complete financial dataset."""
        
        accounts = self.generate_accounts(num_accounts)
        account_ids = [a["account_number"] for a in accounts]
        customer_ids = list(set(a["customer_id"] for a in accounts))
        
        return {
            "accounts": accounts,
            "transactions": self.generate_transactions(
                account_ids, int(num_accounts * transactions_per_account)
            ),
            "investments": self.generate_investments(
                account_ids, int(num_accounts * investments_per_account)
            ),
            "loan_applications": self.generate_loan_applications(num_loan_applications),
            "credit_scores": self.generate_credit_scores(
                customer_ids, int(len(customer_ids) * credit_scores_per_customer)
            ),
            "financial_statements": self.generate_financial_statements(num_financial_statements),
        }
