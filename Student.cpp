#include "Student.h"

// Default Constructor
Student::Student() : m_id(0), m_name(""), m_level(""), m_major(""), m_gpa(0.0), m_advisor(0) {}

// Parameterized Constructor
Student::Student(int id, std::string name, std::string level, std::string major, double gpa, int advisor)
    : m_id(id), m_name(name), m_level(level), m_major(major), m_gpa(gpa), m_advisor(advisor) {}

std::ostream& operator<<(std::ostream& os, const Student& student) {
    os << "ID: " << student.getID()
       << ", Name: " << student.getName()
       << ", Level: " << student.getLevel()
       << ", Major: " << student.getMajor()
       << ", GPA: " << student.getGPA()
       << ", Advisor: " << student.getAdvisor();
    return os;
}

// Destructor
Student::~Student() {}
