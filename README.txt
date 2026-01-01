================================================================================
                    PA5 - LAZY BST DATABASE SYSTEM
================================================================================

Full name: Julian Carbajal
Student ID: 002401916
Chapman email: jcarbajal@chapman.edu
Course number and section: CPSC-350-03
Assignment: PA5 - Lazy BST Database

================================================================================
                           PROGRAM DESCRIPTION
================================================================================

This program implements a university database system using Binary Search Trees
to store and manage Student and Faculty records. The system supports:

OPERATIONS:
- Add/Delete students and faculty
- Search by ID
- Display all records (sorted by ID)
- Track student-advisor relationships

DATA STRUCTURES:
- LazyBST: Template Binary Search Tree
- TreeNode: Individual tree node
- Student: Student record with ID, name, level, major, GPA, advisor
- Faculty: Faculty record with ID, name, level, department, advisees

================================================================================
                           FILE STRUCTURE
================================================================================

ARCHITECTURE:
    main.cpp           <- Entry point, demo operations
        |
        v
    DBsystem (.h/.cpp) <- Database controller with two BSTs
        |
        v
    LazyBST.h          <- Template Binary Search Tree
        |
        v
    TreeNode.h         <- Tree node template
        |
    Student (.h/.cpp)  <- Student record class
    Faculty (.h/.cpp)  <- Faculty record class

FILE DESCRIPTIONS:
    - main.cpp       : Demo program showing database operations
    - DBsystem.h/cpp : Database system managing student/faculty trees
    - LazyBST.h      : Template BST with insert, search, remove, print
    - TreeNode.h     : Template tree node class
    - Student.h/cpp  : Student class with comparison operators
    - Faculty.h/cpp  : Faculty class with advisee management

================================================================================
                           HOW TO BUILD & RUN
================================================================================

COMPILE:
    g++ -std=c++11 -o main main.cpp DBsystem.cpp Student.cpp Faculty.cpp

RUN:
    ./main

The demo program:
1. Adds 15 students and 15 faculty members
2. Displays all students (sorted by ID)
3. Displays all faculty (sorted by ID)
4. Demonstrates find and delete operations

================================================================================
                           SOURCES USED
================================================================================

Books:
    - C++ Crash Course by Josh Lospinoso

Websites:
    - https://stackoverflow.com/
    - https://www.w3schools.com/cpp/
    - https://cplusplus.com/doc/tutorial/files/

================================================================================
                           KNOWN ISSUES / NOTES
================================================================================

- Program compiles and runs successfully
- BST operations (insert, search, delete, print) work correctly
- Students and Faculty are stored in separate BSTs
- Records are printed in sorted order (by ID)