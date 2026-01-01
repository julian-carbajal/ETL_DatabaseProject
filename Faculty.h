#ifndef FACULTY_H
#define FACULTY_H

#include <iostream>
#include <string>
#include "LazyBST.h"

class Faculty
{
public:
    Faculty();
    Faculty(int id, std::string name, std::string level, std::string department);
    ~Faculty();

    bool operator==(const Faculty &other) const { return m_id == other.m_id; }
    bool operator!=(const Faculty &other) const { return m_id != other.m_id; }
    bool operator<(const Faculty &other) const { return m_id < other.m_id; }
    bool operator>(const Faculty &other) const { return m_id > other.m_id; }

    int getID() const { return m_id; }
    void setID(int id) { m_id = id; }
    std::string getName() const { return m_name; }
    void setName(const std::string &name) { m_name = name; }
    std::string getLevel() const { return m_level; }
    void setLevel(const std::string &level) { m_level = level; }
    std::string getDepartment() const { return m_department; }
    void setDepartment(const std::string &department) { m_department = department; }

    void addAdvisee(int adviseeId);
    void removeAdvisee(int adviseeId);
    void printAdvisees() const;

    friend std::ostream &operator<<(std::ostream &os, const Faculty &faculty);

private:
    int m_id;
    std::string m_name;
    std::string m_level;
    std::string m_department;
    static const int MAX_ADVISEES = 100; // Maximum number of advisees
    int m_advisees[MAX_ADVISEES];        // Array to store advisee IDs
    int m_adviseeCount;                  // Current count of advisees
};

#endif // FACULTY_H
