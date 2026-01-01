"""
University Domain Data Generator.
Generates realistic academic data including students, faculty, courses, enrollments.
"""

import random
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
import json

from .base import BaseDataGenerator


class UniversityDataGenerator(BaseDataGenerator):
    """Generate realistic university/academic data."""
    
    def __init__(self, seed: Optional[int] = None):
        super().__init__(seed)
        
        # Academic departments
        self.departments = [
            ("CS", "Computer Science", "College of Engineering"),
            ("MATH", "Mathematics", "College of Arts and Sciences"),
            ("PHYS", "Physics", "College of Arts and Sciences"),
            ("CHEM", "Chemistry", "College of Arts and Sciences"),
            ("BIO", "Biology", "College of Arts and Sciences"),
            ("EE", "Electrical Engineering", "College of Engineering"),
            ("ME", "Mechanical Engineering", "College of Engineering"),
            ("CE", "Civil Engineering", "College of Engineering"),
            ("ECON", "Economics", "College of Business"),
            ("FIN", "Finance", "College of Business"),
            ("MGMT", "Management", "College of Business"),
            ("PSYCH", "Psychology", "College of Arts and Sciences"),
            ("ENGL", "English", "College of Arts and Sciences"),
            ("HIST", "History", "College of Arts and Sciences"),
            ("POLS", "Political Science", "College of Arts and Sciences"),
        ]
        
        # Course templates per department
        self.course_templates = {
            "CS": [
                ("101", "Introduction to Computer Science", 3),
                ("201", "Data Structures", 3),
                ("301", "Algorithms", 3),
                ("350", "Database Systems", 3),
                ("370", "Computer Networks", 3),
                ("401", "Operating Systems", 3),
                ("450", "Machine Learning", 3),
                ("480", "Software Engineering", 3),
            ],
            "MATH": [
                ("101", "Calculus I", 4),
                ("102", "Calculus II", 4),
                ("201", "Linear Algebra", 3),
                ("301", "Differential Equations", 3),
                ("350", "Probability and Statistics", 3),
                ("401", "Real Analysis", 3),
            ],
            "PHYS": [
                ("101", "General Physics I", 4),
                ("102", "General Physics II", 4),
                ("201", "Modern Physics", 3),
                ("301", "Quantum Mechanics", 3),
            ],
            "ECON": [
                ("101", "Principles of Microeconomics", 3),
                ("102", "Principles of Macroeconomics", 3),
                ("301", "Intermediate Microeconomics", 3),
                ("401", "Econometrics", 3),
            ],
            "FIN": [
                ("201", "Financial Accounting", 3),
                ("301", "Corporate Finance", 3),
                ("401", "Investment Analysis", 3),
                ("450", "Financial Modeling", 3),
            ],
        }
        
        # Academic levels
        self.academic_levels = ["FRESHMAN", "SOPHOMORE", "JUNIOR", "SENIOR", "GRADUATE"]
        
        # Degree types
        self.degree_types = ["BS", "BA", "MS", "MBA", "PhD"]
        
        # Faculty ranks
        self.faculty_ranks = [
            ("PROFESSOR", 0.25),
            ("ASSOCIATE_PROFESSOR", 0.30),
            ("ASSISTANT_PROFESSOR", 0.30),
            ("LECTURER", 0.10),
            ("ADJUNCT", 0.05),
        ]
        
        # Funding agencies
        self.funding_agencies = [
            "National Science Foundation (NSF)",
            "National Institutes of Health (NIH)",
            "Department of Energy (DOE)",
            "Department of Defense (DOD)",
            "DARPA",
            "NASA",
            "Private Foundation",
            "Industry Partner",
        ]
        
        # Research areas
        self.research_areas = [
            "Artificial Intelligence", "Machine Learning", "Data Science",
            "Cybersecurity", "Computer Vision", "Natural Language Processing",
            "Quantum Computing", "Bioinformatics", "Robotics",
            "Climate Science", "Renewable Energy", "Materials Science",
            "Neuroscience", "Genomics", "Drug Discovery",
        ]
        
        # Grade distribution (realistic curve)
        self.grades = [
            ("A", 4.0, 0.15),
            ("A-", 3.7, 0.12),
            ("B+", 3.3, 0.15),
            ("B", 3.0, 0.18),
            ("B-", 2.7, 0.12),
            ("C+", 2.3, 0.10),
            ("C", 2.0, 0.08),
            ("C-", 1.7, 0.05),
            ("D", 1.0, 0.03),
            ("F", 0.0, 0.02),
        ]
    
    def generate(self, count: int) -> List[Dict[str, Any]]:
        """Generate student records."""
        return self.generate_students(count)
    
    def generate_departments(self) -> List[Dict[str, Any]]:
        """Generate department records."""
        departments = []
        
        for code, name, college in self.departments:
            department = {
                "department_id": self.random_id("DEPT"),
                "name": name,
                "code": code,
                "college": college,
                "email": f"{code.lower()}@university.edu",
                "phone": self.random_phone(),
                "building": random.choice([
                    "Science Hall", "Engineering Building", "Business Center",
                    "Liberal Arts Building", "Research Tower", "Main Hall"
                ]),
                "room": f"{random.randint(100, 500)}",
                "annual_budget": round(random.uniform(500000, 5000000), 2),
                "research_budget": round(random.uniform(100000, 2000000), 2),
                "faculty_count": random.randint(10, 50),
                "student_count": random.randint(100, 1000),
                "accreditation_status": "ACCREDITED",
                "is_active": True,
                "established_date": self.random_date(
                    date(1950, 1, 1),
                    date(2010, 1, 1)
                ).isoformat(),
                "created_at": datetime.utcnow().isoformat(),
            }
            departments.append(department)
        
        return departments
    
    def generate_students(
        self,
        count: int,
        department_ids: List[str] = None
    ) -> List[Dict[str, Any]]:
        """Generate student records."""
        students = []
        
        for i in range(count):
            first_name, last_name = self.random_name()
            address = self.random_address()
            
            dept_code, dept_name, college = random.choice(self.departments)
            academic_level = self.weighted_choice(
                self.academic_levels,
                [0.25, 0.25, 0.22, 0.20, 0.08]
            )
            
            # Credits based on level
            if academic_level == "FRESHMAN":
                credits = random.randint(0, 30)
            elif academic_level == "SOPHOMORE":
                credits = random.randint(30, 60)
            elif academic_level == "JUNIOR":
                credits = random.randint(60, 90)
            elif academic_level == "SENIOR":
                credits = random.randint(90, 130)
            else:  # GRADUATE
                credits = random.randint(0, 60)
            
            # GPA with realistic distribution
            gpa = min(4.0, max(0.0, random.gauss(3.2, 0.5)))
            
            admission_date = self.random_date(
                date(2018, 8, 1),
                date(2024, 8, 1)
            )
            
            student = {
                "student_id": self.random_id("STU"),
                "first_name": first_name,
                "last_name": last_name,
                "middle_name": random.choice([None, random.choice(self.first_names)]),
                "date_of_birth": self.random_date(
                    date(1995, 1, 1),
                    date(2006, 12, 31)
                ).isoformat(),
                "gender": random.choice(["Male", "Female", "Non-binary", "Prefer not to say"]),
                "email": f"{first_name.lower()}.{last_name.lower()}@student.university.edu",
                "personal_email": self.random_email(first_name, last_name),
                "phone": self.random_phone(),
                **address,
                "department_id": random.choice(department_ids) if department_ids else None,
                "major": dept_name,
                "minor": random.choice([None, random.choice(self.departments)[1]]),
                "degree_type": "BS" if academic_level != "GRADUATE" else random.choice(["MS", "PhD"]),
                "academic_level": academic_level,
                "enrollment_status": self.weighted_choice(
                    ["ACTIVE", "GRADUATED", "SUSPENDED", "WITHDRAWN", "ON_LEAVE"],
                    [0.85, 0.08, 0.02, 0.03, 0.02]
                ),
                "gpa": round(gpa, 2),
                "cumulative_gpa": round(gpa + random.uniform(-0.2, 0.2), 2),
                "credits_earned": credits,
                "credits_attempted": credits + random.randint(0, 6),
                "credits_in_progress": random.randint(12, 18) if academic_level != "GRADUATE" else random.randint(6, 12),
                "admission_date": admission_date.isoformat(),
                "expected_graduation": (
                    admission_date + timedelta(days=365 * (4 if academic_level != "GRADUATE" else 2))
                ).isoformat(),
                "tuition_status": self.weighted_choice(
                    ["PAID", "PENDING", "DELINQUENT"],
                    [0.85, 0.12, 0.03]
                ),
                "financial_aid_status": self.weighted_choice(
                    ["APPROVED", "PENDING", "NOT_APPLIED", "DENIED"],
                    [0.45, 0.15, 0.35, 0.05]
                ),
                "scholarship_amount": round(random.uniform(0, 25000), 2) if random.random() > 0.5 else 0,
                "is_international": random.random() < 0.15,
                "is_transfer": random.random() < 0.20,
                "is_honors": gpa >= 3.5 and random.random() < 0.3,
                "is_athlete": random.random() < 0.08,
                "created_at": datetime.utcnow().isoformat(),
            }
            students.append(student)
        
        return students
    
    def generate_faculty(
        self,
        count: int,
        department_ids: List[str] = None
    ) -> List[Dict[str, Any]]:
        """Generate faculty records."""
        faculty = []
        
        for i in range(count):
            first_name, last_name = self.random_name()
            dept_code, dept_name, college = random.choice(self.departments)
            
            rank = self.weighted_choice(
                [r[0] for r in self.faculty_ranks],
                [r[1] for r in self.faculty_ranks]
            )
            
            # Salary based on rank
            if rank == "PROFESSOR":
                salary = random.uniform(120000, 200000)
            elif rank == "ASSOCIATE_PROFESSOR":
                salary = random.uniform(90000, 140000)
            elif rank == "ASSISTANT_PROFESSOR":
                salary = random.uniform(70000, 110000)
            else:
                salary = random.uniform(45000, 80000)
            
            hire_date = self.random_date(
                date(1990, 1, 1),
                date(2023, 8, 1)
            )
            
            faculty_member = {
                "faculty_id": self.random_id("FAC"),
                "first_name": first_name,
                "last_name": last_name,
                "title": "Dr." if rank != "ADJUNCT" else "Prof.",
                "email": f"{first_name.lower()}.{last_name.lower()}@university.edu",
                "phone": self.random_phone(),
                "office_location": f"{random.choice(['Science Hall', 'Engineering Building', 'Research Tower'])} {random.randint(100, 500)}",
                "office_hours": f"{random.choice(['Mon/Wed', 'Tue/Thu'])} {random.randint(9, 14)}:00-{random.randint(15, 17)}:00",
                "department_id": random.choice(department_ids) if department_ids else None,
                "rank": rank,
                "tenure_status": self.weighted_choice(
                    ["TENURED", "TENURE_TRACK", "NON_TENURE"],
                    [0.35, 0.30, 0.35]
                ) if rank != "ADJUNCT" else "NON_TENURE",
                "hire_date": hire_date.isoformat(),
                "employment_type": "FULL_TIME" if rank != "ADJUNCT" else "PART_TIME",
                "salary": round(salary, 2),
                "research_interests": ", ".join(random.sample(self.research_areas, k=random.randint(2, 4))),
                "publications_count": random.randint(5, 150) if rank in ["PROFESSOR", "ASSOCIATE_PROFESSOR"] else random.randint(0, 30),
                "h_index": random.randint(5, 50) if rank in ["PROFESSOR", "ASSOCIATE_PROFESSOR"] else random.randint(0, 15),
                "is_active": True,
                "is_department_chair": random.random() < 0.05,
                "created_at": datetime.utcnow().isoformat(),
            }
            faculty.append(faculty_member)
        
        return faculty
    
    def generate_courses(
        self,
        department_ids: List[str] = None,
        faculty_ids: List[str] = None,
        terms: List[str] = None
    ) -> List[Dict[str, Any]]:
        """Generate course records."""
        courses = []
        
        if terms is None:
            terms = ["FALL_2023", "SPRING_2024", "FALL_2024"]
        
        for dept_code, dept_name, college in self.departments:
            templates = self.course_templates.get(dept_code, [
                ("101", f"Introduction to {dept_name}", 3),
                ("201", f"Intermediate {dept_name}", 3),
                ("301", f"Advanced {dept_name}", 3),
            ])
            
            for term in terms:
                for course_num, title, credits in templates:
                    # Generate 1-3 sections per course
                    num_sections = random.randint(1, 3)
                    
                    for section in range(1, num_sections + 1):
                        year = int(term.split("_")[1])
                        semester = term.split("_")[0]
                        
                        if semester == "FALL":
                            start_date = date(year, 8, 25)
                            end_date = date(year, 12, 15)
                        else:
                            start_date = date(year, 1, 15)
                            end_date = date(year, 5, 10)
                        
                        capacity = random.choice([25, 30, 35, 40, 50, 100, 200])
                        enrolled = random.randint(int(capacity * 0.5), capacity)
                        
                        course = {
                            "course_id": self.random_id("CRS"),
                            "department_id": random.choice(department_ids) if department_ids else None,
                            "instructor_id": random.choice(faculty_ids) if faculty_ids else None,
                            "course_code": f"{dept_code}{course_num}",
                            "section": f"{section:02d}",
                            "title": title,
                            "description": f"This course covers {title.lower()} concepts and applications.",
                            "credits": credits,
                            "level": "UNDERGRADUATE" if int(course_num) < 500 else "GRADUATE",
                            "term": term,
                            "academic_year": year,
                            "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat(),
                            "days": random.choice(["MWF", "TTH", "MW", "TF"]),
                            "start_time": random.choice(["08:00", "09:30", "11:00", "13:00", "14:30", "16:00"]),
                            "end_time": random.choice(["09:15", "10:45", "12:15", "14:15", "15:45", "17:15"]),
                            "location": f"{random.choice(['Science Hall', 'Engineering Building', 'Lecture Hall'])} {random.randint(100, 300)}",
                            "capacity": capacity,
                            "enrolled_count": enrolled,
                            "waitlist_count": random.randint(0, 10) if enrolled >= capacity else 0,
                            "prerequisites": f"{dept_code}{int(course_num) - 100}" if int(course_num) > 101 else None,
                            "grade_type": "LETTER",
                            "status": "COMPLETED" if end_date < date.today() else "ACTIVE",
                            "is_online": random.random() < 0.15,
                            "is_hybrid": random.random() < 0.10,
                            "lab_fee": round(random.uniform(50, 200), 2) if "Lab" in title or dept_code in ["CHEM", "PHYS", "BIO"] else None,
                            "created_at": datetime.utcnow().isoformat(),
                        }
                        courses.append(course)
        
        return courses
    
    def generate_enrollments(
        self,
        student_ids: List[str],
        course_ids: List[str],
        count: int
    ) -> List[Dict[str, Any]]:
        """Generate enrollment records."""
        enrollments = []
        used_pairs = set()
        
        for i in range(count):
            # Ensure unique student-course pairs
            attempts = 0
            while attempts < 100:
                student_id = random.choice(student_ids)
                course_id = random.choice(course_ids)
                pair = (student_id, course_id)
                
                if pair not in used_pairs:
                    used_pairs.add(pair)
                    break
                attempts += 1
            else:
                continue
            
            # Select grade based on distribution
            grade_info = self.weighted_choice(
                self.grades,
                [g[2] for g in self.grades]
            )
            grade, grade_points, _ = grade_info
            
            enrollment_date = self.random_datetime(
                datetime(2023, 8, 1),
                datetime(2024, 9, 1)
            )
            
            status = self.weighted_choice(
                ["COMPLETED", "ENROLLED", "DROPPED", "WITHDRAWN"],
                [0.75, 0.15, 0.07, 0.03]
            )
            
            enrollment = {
                "enrollment_id": self.random_id("ENR"),
                "student_id": student_id,
                "course_id": course_id,
                "enrollment_date": enrollment_date.isoformat(),
                "drop_date": (
                    (enrollment_date + timedelta(days=random.randint(7, 60))).isoformat()
                    if status in ["DROPPED", "WITHDRAWN"] else None
                ),
                "status": status,
                "grade": grade if status == "COMPLETED" else None,
                "grade_points": grade_points if status == "COMPLETED" else None,
                "grade_type": "LETTER",
                "attendance_pct": round(random.uniform(70, 100), 2) if status != "DROPPED" else round(random.uniform(30, 70), 2),
                "absences": random.randint(0, 10),
                "academic_integrity_violation": random.random() < 0.02,
                "is_repeat": random.random() < 0.08,
                "credits_attempted": random.choice([3, 4]),
                "credits_earned": random.choice([3, 4]) if status == "COMPLETED" and grade != "F" else 0,
                "created_at": datetime.utcnow().isoformat(),
            }
            enrollments.append(enrollment)
        
        return enrollments
    
    def generate_research_grants(
        self,
        faculty_ids: List[str],
        count: int
    ) -> List[Dict[str, Any]]:
        """Generate research grant records."""
        grants = []
        
        for i in range(count):
            agency = random.choice(self.funding_agencies)
            
            # Amount based on agency
            if "NSF" in agency or "NIH" in agency:
                amount = random.uniform(100000, 2000000)
            elif "DOD" in agency or "DARPA" in agency:
                amount = random.uniform(500000, 5000000)
            else:
                amount = random.uniform(50000, 500000)
            
            start_date = self.random_date(
                date(2020, 1, 1),
                date(2024, 6, 1)
            )
            
            duration_years = random.choice([1, 2, 3, 4, 5])
            end_date = start_date + timedelta(days=365 * duration_years)
            
            status = "ACTIVE" if start_date <= date.today() <= end_date else (
                "COMPLETED" if end_date < date.today() else "PENDING"
            )
            
            spent_pct = random.uniform(0.3, 0.9) if status == "ACTIVE" else (
                random.uniform(0.9, 1.0) if status == "COMPLETED" else 0
            )
            
            grant = {
                "grant_id": self.random_id("GRT"),
                "pi_id": random.choice(faculty_ids),
                "title": f"{random.choice(self.research_areas)}: {random.choice(['Novel Approaches', 'Advanced Methods', 'Innovative Solutions', 'Computational Framework'])} for {random.choice(['Healthcare', 'Industry', 'Society', 'Science'])}",
                "abstract": "This research project aims to advance the field through innovative methodologies and cutting-edge techniques.",
                "funding_agency": agency,
                "grant_number": f"{agency[:3].upper()}-{random.randint(1000000, 9999999)}",
                "total_amount": round(amount, 2),
                "amount_spent": round(amount * spent_pct, 2),
                "amount_remaining": round(amount * (1 - spent_pct), 2),
                "indirect_cost_rate": round(random.uniform(0.4, 0.55), 2),
                "submission_date": (start_date - timedelta(days=random.randint(90, 365))).isoformat(),
                "award_date": (start_date - timedelta(days=random.randint(30, 60))).isoformat() if status != "PENDING" else None,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "status": status,
                "research_area": random.choice(self.research_areas),
                "keywords": json.dumps(random.sample(self.research_areas, k=3)),
                "irb_approval": random.random() < 0.3,
                "publications_required": random.randint(2, 10),
                "publications_completed": random.randint(0, 8) if status in ["ACTIVE", "COMPLETED"] else 0,
                "created_at": datetime.utcnow().isoformat(),
            }
            grants.append(grant)
        
        return grants
    
    def generate_complete_dataset(
        self,
        num_students: int = 500,
        num_faculty: int = 100,
        enrollments_per_student: float = 8.0,
        grants_per_faculty: float = 0.5,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Generate a complete university dataset."""
        
        departments = self.generate_departments()
        department_ids = [d["department_id"] for d in departments]
        
        faculty = self.generate_faculty(num_faculty, department_ids)
        faculty_ids = [f["faculty_id"] for f in faculty]
        
        students = self.generate_students(num_students, department_ids)
        student_ids = [s["student_id"] for s in students]
        
        courses = self.generate_courses(department_ids, faculty_ids)
        course_ids = [c["course_id"] for c in courses]
        
        return {
            "departments": departments,
            "faculty": faculty,
            "students": students,
            "courses": courses,
            "enrollments": self.generate_enrollments(
                student_ids, course_ids, int(num_students * enrollments_per_student)
            ),
            "research_grants": self.generate_research_grants(
                faculty_ids, int(num_faculty * grants_per_faculty)
            ),
        }
