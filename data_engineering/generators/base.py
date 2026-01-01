"""
Base data generator with common utilities.
"""

import random
import string
import hashlib
from datetime import datetime, date, timedelta
from typing import List, Optional, Any, Dict
from abc import ABC, abstractmethod


class BaseDataGenerator(ABC):
    """Abstract base class for domain-specific data generators."""
    
    def __init__(self, seed: Optional[int] = None):
        """Initialize generator with optional seed for reproducibility."""
        self.seed = seed
        if seed is not None:
            random.seed(seed)
        
        # Common data pools
        self.first_names = [
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
            "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
            "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
            "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
            "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
            "Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
            "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
            "Wei", "Mei", "Raj", "Priya", "Mohammed", "Fatima", "Carlos", "Maria", "Hiroshi", "Yuki",
        ]
        
        self.last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
            "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
            "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
            "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
            "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
            "Chen", "Wang", "Kim", "Patel", "Singh", "Kumar", "Tanaka", "Yamamoto", "Mueller",
        ]
        
        self.cities = [
            ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"), ("Houston", "TX"),
            ("Phoenix", "AZ"), ("Philadelphia", "PA"), ("San Antonio", "TX"), ("San Diego", "CA"),
            ("Dallas", "TX"), ("San Jose", "CA"), ("Austin", "TX"), ("Jacksonville", "FL"),
            ("Fort Worth", "TX"), ("Columbus", "OH"), ("Charlotte", "NC"), ("San Francisco", "CA"),
            ("Indianapolis", "IN"), ("Seattle", "WA"), ("Denver", "CO"), ("Boston", "MA"),
            ("Portland", "OR"), ("Atlanta", "GA"), ("Miami", "FL"), ("Detroit", "MI"),
        ]
        
        self.email_domains = [
            "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com",
            "protonmail.com", "aol.com", "mail.com",
        ]
    
    @abstractmethod
    def generate(self, count: int) -> List[Dict[str, Any]]:
        """Generate specified number of records."""
        pass
    
    def random_id(self, prefix: str, length: int = 8) -> str:
        """Generate a random ID with prefix."""
        digits = ''.join(random.choices(string.digits, k=length))
        return f"{prefix}{digits}"
    
    def random_name(self) -> tuple:
        """Generate random first and last name."""
        return random.choice(self.first_names), random.choice(self.last_names)
    
    def random_email(self, first_name: str, last_name: str) -> str:
        """Generate email from name."""
        domain = random.choice(self.email_domains)
        formats = [
            f"{first_name.lower()}.{last_name.lower()}@{domain}",
            f"{first_name.lower()}{last_name.lower()}@{domain}",
            f"{first_name[0].lower()}{last_name.lower()}@{domain}",
            f"{first_name.lower()}{random.randint(1, 99)}@{domain}",
        ]
        return random.choice(formats)
    
    def random_phone(self) -> str:
        """Generate random US phone number."""
        area = random.randint(200, 999)
        exchange = random.randint(200, 999)
        subscriber = random.randint(1000, 9999)
        return f"{area}-{exchange}-{subscriber}"
    
    def random_address(self) -> Dict[str, str]:
        """Generate random address."""
        city, state = random.choice(self.cities)
        street_num = random.randint(100, 9999)
        street_names = ["Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Park Blvd", 
                       "Washington St", "Lake Rd", "Hill Ave", "River Dr", "Forest Way"]
        
        return {
            "address_line1": f"{street_num} {random.choice(street_names)}",
            "address_line2": random.choice(["", "", "", f"Apt {random.randint(1, 500)}", f"Suite {random.randint(100, 999)}"]),
            "city": city,
            "state": state,
            "zip_code": f"{random.randint(10000, 99999)}",
            "country": "USA",
        }
    
    def random_date(self, start_date: date, end_date: date) -> date:
        """Generate random date between start and end."""
        delta = (end_date - start_date).days
        random_days = random.randint(0, delta)
        return start_date + timedelta(days=random_days)
    
    def random_datetime(self, start: datetime, end: datetime) -> datetime:
        """Generate random datetime between start and end."""
        delta = end - start
        random_seconds = random.randint(0, int(delta.total_seconds()))
        return start + timedelta(seconds=random_seconds)
    
    def hash_value(self, value: str) -> str:
        """Create SHA-256 hash of value."""
        return hashlib.sha256(value.encode()).hexdigest()
    
    def weighted_choice(self, choices: List[Any], weights: List[float]) -> Any:
        """Make weighted random choice."""
        return random.choices(choices, weights=weights, k=1)[0]
    
    def random_ssn(self) -> str:
        """Generate random SSN format (not real)."""
        return f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"
