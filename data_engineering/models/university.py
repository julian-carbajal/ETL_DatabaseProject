"""
University Domain Models - Academic Data Management.
Covers: Students, faculty, courses, enrollments, departments, research grants.
"""

from datetime import datetime, date
from typing import Optional
from sqlalchemy import (
    Column, Integer, String, DateTime, Date, Float, Boolean,
    Text, ForeignKey, Numeric, Index, CheckConstraint, UniqueConstraint, JSON
)
from sqlalchemy.orm import relationship
import enum

from .base import Base, TimestampMixin, AuditMixin, DataLineageMixin


class EnrollmentStatus(enum.Enum):
    ACTIVE = "ACTIVE"
    GRADUATED = "GRADUATED"
    SUSPENDED = "SUSPENDED"
    WITHDRAWN = "WITHDRAWN"
    ON_LEAVE = "ON_LEAVE"
    DISMISSED = "DISMISSED"


class CourseStatus(enum.Enum):
    ACTIVE = "ACTIVE"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"


class GradeType(enum.Enum):
    LETTER = "LETTER"
    PASS_FAIL = "PASS_FAIL"
    AUDIT = "AUDIT"


class Student(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """
    Student master record - Central entity for university domain.
    Tracks academic progress, enrollment, and demographics.
    """
    __tablename__ = "university_students"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String(20), unique=True, nullable=False, index=True)  # STU00000001
    
    # Personal Information
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    middle_name = Column(String(100), nullable=True)
    preferred_name = Column(String(100), nullable=True)
    date_of_birth = Column(Date, nullable=True)
    gender = Column(String(20), nullable=True)
    
    # Contact Information
    email = Column(String(255), nullable=False, unique=True)
    personal_email = Column(String(255), nullable=True)
    phone = Column(String(20), nullable=True)
    address_line1 = Column(String(255), nullable=True)
    address_line2 = Column(String(255), nullable=True)
    city = Column(String(100), nullable=True)
    state = Column(String(50), nullable=True)
    zip_code = Column(String(20), nullable=True)
    country = Column(String(100), default="USA")
    
    # Academic Information
    department_id = Column(Integer, ForeignKey("university_departments.id"), nullable=True)
    major = Column(String(200), nullable=True)
    minor = Column(String(200), nullable=True)
    concentration = Column(String(200), nullable=True)
    degree_type = Column(String(50), nullable=True)  # BS, BA, MS, MBA, PhD
    
    # Academic Standing
    academic_level = Column(String(20), nullable=True)  # FRESHMAN, SOPHOMORE, JUNIOR, SENIOR, GRADUATE
    enrollment_status = Column(String(20), default="ACTIVE")
    gpa = Column(Numeric(3, 2), nullable=True)
    cumulative_gpa = Column(Numeric(3, 2), nullable=True)
    credits_earned = Column(Integer, default=0)
    credits_attempted = Column(Integer, default=0)
    credits_in_progress = Column(Integer, default=0)
    
    # Dates
    admission_date = Column(Date, nullable=True)
    expected_graduation = Column(Date, nullable=True)
    actual_graduation = Column(Date, nullable=True)
    
    # Advisor
    advisor_id = Column(Integer, ForeignKey("university_faculty.id"), nullable=True)
    
    # Financial
    tuition_status = Column(String(20), nullable=True)  # PAID, PENDING, DELINQUENT
    financial_aid_status = Column(String(20), nullable=True)
    scholarship_amount = Column(Numeric(12, 2), nullable=True)
    
    # Flags
    is_international = Column(Boolean, default=False)
    is_transfer = Column(Boolean, default=False)
    is_honors = Column(Boolean, default=False)
    is_athlete = Column(Boolean, default=False)
    
    # Relationships
    department = relationship("Department", back_populates="students")
    advisor = relationship("Faculty", back_populates="advisees", foreign_keys=[advisor_id])
    enrollments = relationship("Enrollment", back_populates="student", lazy="dynamic")
    
    __table_args__ = (
        Index("idx_student_name", "last_name", "first_name"),
        Index("idx_student_major", "major", "academic_level"),
        Index("idx_student_status", "enrollment_status", "academic_level"),
        CheckConstraint("gpa >= 0 AND gpa <= 4.0", name="check_gpa_range"),
        CheckConstraint("credits_earned >= 0", name="check_credits_positive"),
    )
    
    def __repr__(self):
        return f"<Student(id={self.student_id}, name={self.last_name}, {self.first_name})>"


class Faculty(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """Faculty member records."""
    __tablename__ = "university_faculty"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    faculty_id = Column(String(20), unique=True, nullable=False, index=True)  # FAC00000001
    
    # Personal Information
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    title = Column(String(50), nullable=True)  # Dr., Prof.
    
    # Contact
    email = Column(String(255), nullable=False, unique=True)
    phone = Column(String(20), nullable=True)
    office_location = Column(String(100), nullable=True)
    office_hours = Column(String(500), nullable=True)
    
    # Academic Position
    department_id = Column(Integer, ForeignKey("university_departments.id"), nullable=True)
    rank = Column(String(50), nullable=True)  # PROFESSOR, ASSOCIATE_PROFESSOR, ASSISTANT_PROFESSOR, LECTURER
    tenure_status = Column(String(20), nullable=True)  # TENURED, TENURE_TRACK, NON_TENURE
    
    # Employment
    hire_date = Column(Date, nullable=True)
    contract_end_date = Column(Date, nullable=True)
    employment_type = Column(String(20), nullable=True)  # FULL_TIME, PART_TIME, ADJUNCT
    salary = Column(Numeric(12, 2), nullable=True)
    
    # Research
    research_interests = Column(Text, nullable=True)
    publications_count = Column(Integer, default=0)
    h_index = Column(Integer, nullable=True)
    
    # Status
    is_active = Column(Boolean, default=True)
    is_department_chair = Column(Boolean, default=False)
    
    # Relationships
    department = relationship("Department", back_populates="faculty_members")
    advisees = relationship("Student", back_populates="advisor", foreign_keys="Student.advisor_id")
    courses_taught = relationship("Course", back_populates="instructor", lazy="dynamic")
    grants = relationship("ResearchGrant", back_populates="principal_investigator", lazy="dynamic")
    
    __table_args__ = (
        Index("idx_faculty_name", "last_name", "first_name"),
        Index("idx_faculty_dept", "department_id", "rank"),
    )
    
    def __repr__(self):
        return f"<Faculty(id={self.faculty_id}, name={self.last_name})>"


class Department(Base, TimestampMixin, AuditMixin):
    """Academic department information."""
    __tablename__ = "university_departments"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    department_id = Column(String(20), unique=True, nullable=False, index=True)
    
    # Department Info
    name = Column(String(200), nullable=False)
    code = Column(String(10), nullable=False, unique=True)  # CS, MATH, PHYS
    college = Column(String(200), nullable=True)  # College of Engineering, College of Arts
    
    # Contact
    email = Column(String(255), nullable=True)
    phone = Column(String(20), nullable=True)
    building = Column(String(100), nullable=True)
    room = Column(String(20), nullable=True)
    
    # Leadership
    chair_id = Column(Integer, nullable=True)  # FK to faculty
    
    # Budget
    annual_budget = Column(Numeric(14, 2), nullable=True)
    research_budget = Column(Numeric(14, 2), nullable=True)
    
    # Statistics
    faculty_count = Column(Integer, default=0)
    student_count = Column(Integer, default=0)
    course_count = Column(Integer, default=0)
    
    # Accreditation
    accreditation_status = Column(String(50), nullable=True)
    accreditation_date = Column(Date, nullable=True)
    next_review_date = Column(Date, nullable=True)
    
    # Status
    is_active = Column(Boolean, default=True)
    established_date = Column(Date, nullable=True)
    
    # Relationships
    students = relationship("Student", back_populates="department", lazy="dynamic")
    faculty_members = relationship("Faculty", back_populates="department", lazy="dynamic")
    courses = relationship("Course", back_populates="department", lazy="dynamic")
    
    def __repr__(self):
        return f"<Department(code={self.code}, name={self.name})>"


class Course(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """Course catalog and section information."""
    __tablename__ = "university_courses"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    course_id = Column(String(20), unique=True, nullable=False, index=True)
    
    department_id = Column(Integer, ForeignKey("university_departments.id"), nullable=True)
    instructor_id = Column(Integer, ForeignKey("university_faculty.id"), nullable=True)
    
    # Course Identity
    course_code = Column(String(20), nullable=False)  # CS350
    section = Column(String(10), nullable=True)  # 01, 02
    title = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    
    # Credits and Level
    credits = Column(Integer, nullable=False)
    level = Column(String(20), nullable=True)  # UNDERGRADUATE, GRADUATE
    
    # Schedule
    term = Column(String(20), nullable=False)  # FALL_2024, SPRING_2025
    academic_year = Column(Integer, nullable=False)
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)
    
    # Meeting Times
    days = Column(String(20), nullable=True)  # MWF, TTH
    start_time = Column(String(10), nullable=True)  # 09:00
    end_time = Column(String(10), nullable=True)  # 10:15
    location = Column(String(100), nullable=True)
    
    # Enrollment
    capacity = Column(Integer, nullable=True)
    enrolled_count = Column(Integer, default=0)
    waitlist_count = Column(Integer, default=0)
    
    # Requirements
    prerequisites = Column(Text, nullable=True)
    corequisites = Column(Text, nullable=True)
    
    # Grading
    grade_type = Column(String(20), default="LETTER")
    
    # Status
    status = Column(String(20), default="ACTIVE")
    is_online = Column(Boolean, default=False)
    is_hybrid = Column(Boolean, default=False)
    
    # Fees
    lab_fee = Column(Numeric(8, 2), nullable=True)
    material_fee = Column(Numeric(8, 2), nullable=True)
    
    # Relationships
    department = relationship("Department", back_populates="courses")
    instructor = relationship("Faculty", back_populates="courses_taught")
    enrollments = relationship("Enrollment", back_populates="course", lazy="dynamic")
    
    __table_args__ = (
        Index("idx_course_code", "course_code", "term"),
        Index("idx_course_dept", "department_id", "term"),
        Index("idx_course_instructor", "instructor_id", "term"),
        UniqueConstraint("course_code", "section", "term", name="uq_course_section_term"),
    )
    
    def __repr__(self):
        return f"<Course(code={self.course_code}, title={self.title})>"
    
    @property
    def available_seats(self) -> int:
        """Calculate available seats."""
        if self.capacity:
            return max(0, self.capacity - self.enrolled_count)
        return 0


class Enrollment(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """Student course enrollment records."""
    __tablename__ = "university_enrollments"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    enrollment_id = Column(String(20), unique=True, nullable=False, index=True)
    
    student_id = Column(Integer, ForeignKey("university_students.id"), nullable=False)
    course_id = Column(Integer, ForeignKey("university_courses.id"), nullable=False)
    
    # Enrollment Details
    enrollment_date = Column(DateTime, nullable=False)
    drop_date = Column(DateTime, nullable=True)
    
    # Status
    status = Column(String(20), default="ENROLLED")  # ENROLLED, DROPPED, WITHDRAWN, COMPLETED
    
    # Grading
    grade = Column(String(5), nullable=True)  # A, A-, B+, etc.
    grade_points = Column(Numeric(3, 2), nullable=True)  # 4.0, 3.7, 3.3, etc.
    grade_type = Column(String(20), default="LETTER")
    
    # Attendance
    attendance_pct = Column(Numeric(5, 2), nullable=True)
    absences = Column(Integer, default=0)
    
    # Academic Integrity
    academic_integrity_violation = Column(Boolean, default=False)
    violation_notes = Column(Text, nullable=True)
    
    # Repeat
    is_repeat = Column(Boolean, default=False)
    previous_grade = Column(String(5), nullable=True)
    
    # Credits
    credits_attempted = Column(Integer, nullable=True)
    credits_earned = Column(Integer, nullable=True)
    
    # Relationships
    student = relationship("Student", back_populates="enrollments")
    course = relationship("Course", back_populates="enrollments")
    
    __table_args__ = (
        Index("idx_enrollment_student", "student_id", "status"),
        Index("idx_enrollment_course", "course_id", "status"),
        UniqueConstraint("student_id", "course_id", name="uq_student_course"),
    )
    
    def __repr__(self):
        return f"<Enrollment(student={self.student_id}, course={self.course_id})>"


class ResearchGrant(Base, TimestampMixin, AuditMixin, DataLineageMixin):
    """Research grant tracking."""
    __tablename__ = "university_grants"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    grant_id = Column(String(20), unique=True, nullable=False, index=True)
    
    pi_id = Column(Integer, ForeignKey("university_faculty.id"), nullable=False)
    
    # Grant Information
    title = Column(String(500), nullable=False)
    abstract = Column(Text, nullable=True)
    
    # Funding
    funding_agency = Column(String(200), nullable=False)  # NSF, NIH, DOE, DOD
    grant_number = Column(String(100), nullable=True)
    total_amount = Column(Numeric(14, 2), nullable=False)
    amount_spent = Column(Numeric(14, 2), default=0)
    amount_remaining = Column(Numeric(14, 2), nullable=True)
    indirect_cost_rate = Column(Numeric(5, 2), nullable=True)
    
    # Timeline
    submission_date = Column(Date, nullable=True)
    award_date = Column(Date, nullable=True)
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)
    
    # Status
    status = Column(String(20), default="PENDING")  # PENDING, AWARDED, ACTIVE, COMPLETED, REJECTED
    
    # Classification
    research_area = Column(String(200), nullable=True)
    keywords = Column(JSON, nullable=True)
    
    # Co-Investigators (JSON array of faculty IDs)
    co_investigators = Column(JSON, nullable=True)
    
    # Compliance
    irb_approval = Column(Boolean, nullable=True)
    irb_number = Column(String(50), nullable=True)
    iacuc_approval = Column(Boolean, nullable=True)
    
    # Deliverables
    publications_required = Column(Integer, nullable=True)
    publications_completed = Column(Integer, default=0)
    
    # Relationships
    principal_investigator = relationship("Faculty", back_populates="grants")
    
    __table_args__ = (
        Index("idx_grant_pi", "pi_id", "status"),
        Index("idx_grant_agency", "funding_agency", "status"),
        Index("idx_grant_dates", "start_date", "end_date"),
    )
    
    def __repr__(self):
        return f"<ResearchGrant(id={self.grant_id}, title={self.title[:50]})>"
    
    @property
    def burn_rate_pct(self) -> Optional[float]:
        """Calculate percentage of grant spent."""
        if self.total_amount and self.total_amount > 0:
            return float(self.amount_spent / self.total_amount * 100)
        return None
