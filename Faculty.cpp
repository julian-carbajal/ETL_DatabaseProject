#include "Faculty.h"
#include <algorithm>

// Default Constructor
Faculty::Faculty() : m_id(0), m_name(""), m_level(""), m_department(""), m_adviseeCount(0) {}

// Parameterized Constructor
Faculty::Faculty(int id, std::string name, std::string level, std::string department)
    : m_id(id), m_name(name), m_level(level), m_department(department), m_adviseeCount(0) {}

// Destructor
Faculty::~Faculty() {}

// Add advisee ID to the faculty's list
void Faculty::addAdvisee(int adviseeId)
{
    if (m_adviseeCount < MAX_ADVISEES)
    {
        m_advisees[m_adviseeCount++] = adviseeId;
    }
}

// Remove advisee ID from the faculty's list
void Faculty::removeAdvisee(int adviseeId)
{
    int index = -1;
    for (int i = 0; i < m_adviseeCount; i++)
    {
        if (m_advisees[i] == adviseeId)
        {
            index = i;
            break;
        }
    }
    if (index != -1)
    {
        for (int i = index; i < m_adviseeCount - 1; i++)
        {
            m_advisees[i] = m_advisees[i + 1];
        }
        m_adviseeCount--;
    }
}

// Print all advisee IDs
void Faculty::printAdvisees() const
{
    std::cout << "Advisees: ";
    for (int i = 0; i < m_adviseeCount; i++)
    {
        std::cout << m_advisees[i] << " ";
    }
    std::cout << std::endl;
}

// Overload the stream insertion operator to print faculty details
std::ostream &operator<<(std::ostream &os, const Faculty &faculty)
{
    os << "ID: " << faculty.m_id
       << ", Name: " << faculty.m_name
       << ", Level: " << faculty.m_level
       << ", Department: " << faculty.m_department
       << ", Advisees: ";
    for (int i = 0; i < faculty.m_adviseeCount; i++)
    {
        os << faculty.m_advisees[i] << " ";
    }
    return os;
}
