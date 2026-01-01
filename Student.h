#ifndef STUDENT_H
#define STUDENT_H

#include <iostream>
#include <string>
#include <fstream>
#include "LazyBST.h"

class Student
{
public:
    // Constructors and Destructor
    Student();
    Student(int id, std::string name, std::string level, std::string major, double gpa, int advisor);
    ~Student();

    // Comparison operators
    bool operator==(const Student &other) const { return m_id == other.m_id; }
    bool operator!=(const Student &other) const { return m_id != other.m_id; }
    bool operator<(const Student &other) const { return m_id < other.m_id; }
    bool operator>(const Student &other) const { return m_id > other.m_id; }

    // Getters and Setters
    int getID() const { return m_id; }
    void setID(int id) { m_id = id; }
    std::string getName() const { return m_name; }
    void setName(const std::string &name) { m_name = name; }
    std::string getLevel() const { return m_level; }
    void setLevel(const std::string &level) { m_level = level; }
    std::string getMajor() const { return m_major; }
    void setMajor(const std::string &major) { m_major = major; }
    double getGPA() const { return m_gpa; }
    void setGPA(double gpa) { m_gpa = gpa; }
    int getAdvisor() const { return m_advisor; }
    void setAdvisor(int advisor) { m_advisor = advisor; }
    void setAdvisorId(int id) { m_advisor = id; }

    friend std::ostream &operator<<(std::ostream &os, const Student &student);

private:
    int m_id;
    std::string m_name;
    std::string m_level;
    std::string m_major;
    double m_gpa;
    int m_advisor;
};

#endif