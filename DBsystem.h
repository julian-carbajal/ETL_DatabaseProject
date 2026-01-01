#ifndef DBsystem_H
#define DBsystem_H

#include "LazyBST.h"
#include "Student.h"
#include "Faculty.h"

class DBsystem
{
public:
        DBsystem();
        ~DBsystem();

        void addStudent(const Student &student);
        void deleteStudent(int studentId);
        Student *findStudent(int studentId);
        void displayAllStudents();

        void addFaculty(const Faculty &faculty);
        void deleteFaculty(int facultyId);
        Faculty *findFaculty(int facultyId);
        void displayAllFaculty();
        void MainMenu();
        void changeAdvisor(int studentId, int facultyId);
        void removeAdvisee(int studentId, int facultyId);
        friend class LazyBST<Student>;
        friend class LazyBST<Faculty>;
        


private:
        LazyBST<Student> studentTree;
        LazyBST<Faculty> facultyTree;
};

#endif // DBsystem_H
