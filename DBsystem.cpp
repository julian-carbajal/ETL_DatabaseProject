#include "DBsystem.h"

DBsystem::DBsystem()
{
    
}
DBsystem::~DBsystem()
{
    
}

void DBsystem::addStudent(const Student &student)
{
    studentTree.insert(student);
}

void DBsystem::deleteStudent(int studentId)
{
    Student temp(studentId, "", "", "", 0.0, 0);
    studentTree.remove(temp);
}

Student *DBsystem::findStudent(int studentId)
{
    Student temp(studentId, "", "", "", 0.0, 0);
    return studentTree.search(temp);
}

void DBsystem::displayAllStudents()
{
    std::cout << "All Students:" << std::endl;
    studentTree.printInOrder();
}

void DBsystem::addFaculty(const Faculty &faculty)
{
    facultyTree.insert(faculty);
}

void DBsystem::deleteFaculty(int facultyId)
{
    Faculty temp(facultyId, "", "", "");
    facultyTree.remove(temp);
}

Faculty *DBsystem::findFaculty(int facultyId)
{
    Faculty temp(facultyId, "", "", "");
    return facultyTree.search(temp);
}

void DBsystem::displayAllFaculty()
{
    std::cout << "All Faculty:" << std::endl;
    facultyTree.printInOrder();
}

