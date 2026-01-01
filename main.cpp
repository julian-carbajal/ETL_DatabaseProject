/**
 * @file main.cpp
 * @brief Interactive University Database System
 * @author Julian Carbajal
 * @date Spring 2024
 */

#include "DBsystem.h"
#include <iostream>
#include <string>
#include <iomanip>
#include <limits>

using namespace std;

// ANSI color codes
const string RESET = "\033[0m";
const string RED = "\033[31m";
const string GREEN = "\033[32m";
const string YELLOW = "\033[33m";
const string BLUE = "\033[34m";
const string MAGENTA = "\033[35m";
const string CYAN = "\033[36m";
const string BOLD = "\033[1m";
const string DIM = "\033[2m";

void clearScreen() {
    cout << "\033[2J\033[1;1H";
}

void displayBanner() {
    cout << MAGENTA << BOLD;
    cout << "  _   _       _                    _ _           ____  ____  \n";
    cout << " | | | |_ __ (_)_   _____ _ __ ___(_) |_ _   _  |  _ \\| __ ) \n";
    cout << " | | | | '_ \\| \\ \\ / / _ \\ '__/ __| | __| | | | | | | |  _ \\ \n";
    cout << " | |_| | | | | |\\ V /  __/ |  \\__ \\ | |_| |_| | | |_| | |_) |\n";
    cout << "  \\___/|_| |_|_| \\_/ \\___|_|  |___/_|\\__|\\__, | |____/|____/ \n";
    cout << "                                        |___/               \n";
    cout << RESET << "           Student & Faculty Database v2.0\n\n";
}

void displayMainMenu() {
    cout << "\n";
    cout << "╔══════════════════════════════════════════════════════════════╗\n";
    cout << "║              UNIVERSITY DATABASE SYSTEM                      ║\n";
    cout << "╠══════════════════════════════════════════════════════════════╣\n";
    cout << "║  " << CYAN << "STUDENTS" << RESET << "                    " << YELLOW << "FACULTY" << RESET << "                       ║\n";
    cout << "║  1. Add Student           6. Add Faculty                     ║\n";
    cout << "║  2. Find Student          7. Find Faculty                    ║\n";
    cout << "║  3. Delete Student        8. Delete Faculty                  ║\n";
    cout << "║  4. Display All Students  9. Display All Faculty             ║\n";
    cout << "║  5. Student Count        10. Faculty Count                   ║\n";
    cout << "╠══════════════════════════════════════════════════════════════╣\n";
    cout << "║ 11. Load Sample Data     12. Clear Database                  ║\n";
    cout << "║ 13. Database Statistics  14. Exit                            ║\n";
    cout << "╚══════════════════════════════════════════════════════════════╝\n";
    cout << "Enter choice: ";
}

void displayStudentTable(DBsystem& db) {
    cout << "\n" << BOLD << "═══════════════ ALL STUDENTS ═══════════════" << RESET << "\n";
    cout << "┌──────┬────────────────┬────────────┬──────────────────┬──────┬─────────┐\n";
    cout << "│ " << BOLD << "ID" << RESET << "   │ " << BOLD << "Name" << RESET << "           │ " << BOLD << "Level" << RESET << "      │ " << BOLD << "Major" << RESET << "            │ " << BOLD << "GPA" << RESET << "  │ " << BOLD << "Advisor" << RESET << " │\n";
    cout << "├──────┼────────────────┼────────────┼──────────────────┼──────┼─────────┤\n";
    db.displayAllStudents();
    cout << "└──────┴────────────────┴────────────┴──────────────────┴──────┴─────────┘\n";
}

void loadSampleData(DBsystem& db) {
    // Add sample students
    db.addStudent(Student(1, "Alice", "Senior", "Computer Science", 3.9, 101));
    db.addStudent(Student(2, "Bob", "Junior", "Mathematics", 3.5, 102));
    db.addStudent(Student(3, "Charlie", "Sophomore", "Computer Science", 3.8, 101));
    db.addStudent(Student(4, "David", "Freshman", "Mathematics", 3.2, 102));
    db.addStudent(Student(5, "Eve", "Senior", "Computer Science", 3.6, 101));
    db.addStudent(Student(6, "Frank", "Sophomore", "Mathematics", 3.4, 102));
    db.addStudent(Student(7, "Grace", "Freshman", "Computer Science", 3.7, 101));
    db.addStudent(Student(8, "Henry", "Junior", "Mathematics", 3.3, 102));
    
    // Add sample faculty
    db.addFaculty(Faculty(101, "Dr. Smith", "Professor", "Computer Science"));
    db.addFaculty(Faculty(102, "Dr. Johnson", "Associate Professor", "Mathematics"));
    db.addFaculty(Faculty(103, "Dr. Williams", "Assistant Professor", "Computer Science"));
    db.addFaculty(Faculty(104, "Dr. Brown", "Associate Professor", "Mathematics"));
    
    cout << GREEN << "✓ Loaded 8 students and 4 faculty members." << RESET << "\n";
}

void addStudentInteractive(DBsystem& db) {
    int id, advisorId;
    string name, level, major;
    double gpa;
    
    cout << "\n" << BOLD << "═══════════════ ADD NEW STUDENT ═══════════════" << RESET << "\n";
    
    cout << "Enter Student ID: ";
    cin >> id;
    cin.ignore(numeric_limits<streamsize>::max(), '\n');
    
    cout << "Enter Name: ";
    getline(cin, name);
    
    cout << "Enter Level (Freshman/Sophomore/Junior/Senior): ";
    getline(cin, level);
    
    cout << "Enter Major: ";
    getline(cin, major);
    
    cout << "Enter GPA (0.0-4.0): ";
    cin >> gpa;
    
    cout << "Enter Advisor ID: ";
    cin >> advisorId;
    
    db.addStudent(Student(id, name, level, major, gpa, advisorId));
    cout << GREEN << "✓ Student added successfully!" << RESET << "\n";
}

void addFacultyInteractive(DBsystem& db) {
    int id;
    string name, level, department;
    
    cout << "\n" << BOLD << "═══════════════ ADD NEW FACULTY ═══════════════" << RESET << "\n";
    
    cout << "Enter Faculty ID: ";
    cin >> id;
    cin.ignore(numeric_limits<streamsize>::max(), '\n');
    
    cout << "Enter Name: ";
    getline(cin, name);
    
    cout << "Enter Level (Professor/Associate Professor/Assistant Professor): ";
    getline(cin, level);
    
    cout << "Enter Department: ";
    getline(cin, department);
    
    db.addFaculty(Faculty(id, name, level, department));
    cout << GREEN << "✓ Faculty added successfully!" << RESET << "\n";
}

void findStudentInteractive(DBsystem& db) {
    int id;
    cout << "\nEnter Student ID to find: ";
    cin >> id;
    
    Student* s = db.findStudent(id);
    if (s) {
        cout << GREEN << "\n✓ Student Found:" << RESET << "\n";
        cout << "┌──────────────────────────────────────────┐\n";
        cout << "│ ID: " << s->getID() << "\n";
        cout << "│ Name: " << s->getName() << "\n";
        cout << "│ Level: " << s->getLevel() << "\n";
        cout << "│ Major: " << s->getMajor() << "\n";
        cout << "│ GPA: " << fixed << setprecision(2) << s->getGPA() << "\n";
        cout << "│ Advisor ID: " << s->getAdvisor() << "\n";
        cout << "└──────────────────────────────────────────┘\n";
    } else {
        cout << RED << "✗ Student with ID " << id << " not found." << RESET << "\n";
    }
}

void findFacultyInteractive(DBsystem& db) {
    int id;
    cout << "\nEnter Faculty ID to find: ";
    cin >> id;
    
    Faculty* f = db.findFaculty(id);
    if (f) {
        cout << GREEN << "\n✓ Faculty Found:" << RESET << "\n";
        cout << "┌──────────────────────────────────────────┐\n";
        cout << "│ ID: " << f->getID() << "\n";
        cout << "│ Name: " << f->getName() << "\n";
        cout << "│ Level: " << f->getLevel() << "\n";
        cout << "│ Department: " << f->getDepartment() << "\n";
        cout << "└──────────────────────────────────────────┘\n";
    } else {
        cout << RED << "✗ Faculty with ID " << id << " not found." << RESET << "\n";
    }
}

void deleteStudentInteractive(DBsystem& db) {
    int id;
    cout << "\nEnter Student ID to delete: ";
    cin >> id;
    
    Student* s = db.findStudent(id);
    if (s) {
        cout << YELLOW << "Are you sure you want to delete " << s->getName() << "? (y/n): " << RESET;
        char confirm;
        cin >> confirm;
        if (confirm == 'y' || confirm == 'Y') {
            db.deleteStudent(id);
            cout << GREEN << "✓ Student deleted successfully." << RESET << "\n";
        } else {
            cout << "Deletion cancelled.\n";
        }
    } else {
        cout << RED << "✗ Student with ID " << id << " not found." << RESET << "\n";
    }
}

void deleteFacultyInteractive(DBsystem& db) {
    int id;
    cout << "\nEnter Faculty ID to delete: ";
    cin >> id;
    
    Faculty* f = db.findFaculty(id);
    if (f) {
        cout << YELLOW << "Are you sure you want to delete " << f->getName() << "? (y/n): " << RESET;
        char confirm;
        cin >> confirm;
        if (confirm == 'y' || confirm == 'Y') {
            db.deleteFaculty(id);
            cout << GREEN << "✓ Faculty deleted successfully." << RESET << "\n";
        } else {
            cout << "Deletion cancelled.\n";
        }
    } else {
        cout << RED << "✗ Faculty with ID " << id << " not found." << RESET << "\n";
    }
}

void displayStatistics(DBsystem& db) {
    cout << "\n" << BOLD << "═══════════════ DATABASE STATISTICS ═══════════════" << RESET << "\n";
    cout << "┌──────────────────────────────────────────┐\n";
    cout << "│ " << CYAN << "Students in Database:" << RESET << " (see display)\n";
    cout << "│ " << YELLOW << "Faculty in Database:" << RESET << "  (see display)\n";
    cout << "└──────────────────────────────────────────┘\n";
}

int main()
{
    DBsystem db;
    int choice;
    
    clearScreen();
    displayBanner();
    
    while (true) {
        displayMainMenu();
        
        if (!(cin >> choice)) {
            cin.clear();
            cin.ignore(numeric_limits<streamsize>::max(), '\n');
            cout << RED << "Invalid input. Please enter a number." << RESET << "\n";
            continue;
        }
        
        switch (choice) {
            case 1:
                addStudentInteractive(db);
                break;
            case 2:
                findStudentInteractive(db);
                break;
            case 3:
                deleteStudentInteractive(db);
                break;
            case 4:
                cout << "\n" << BOLD << "All Students:" << RESET << "\n";
                db.displayAllStudents();
                break;
            case 5:
                cout << "\n" << CYAN << "Total Students: " << RESET << "(display all to see)\n";
                break;
            case 6:
                addFacultyInteractive(db);
                break;
            case 7:
                findFacultyInteractive(db);
                break;
            case 8:
                deleteFacultyInteractive(db);
                break;
            case 9:
                cout << "\n" << BOLD << "All Faculty:" << RESET << "\n";
                db.displayAllFaculty();
                break;
            case 10:
                cout << "\n" << YELLOW << "Total Faculty: " << RESET << "(display all to see)\n";
                break;
            case 11:
                loadSampleData(db);
                break;
            case 12:
                cout << YELLOW << "Clear database? This cannot be undone. (y/n): " << RESET;
                char confirm;
                cin >> confirm;
                if (confirm == 'y' || confirm == 'Y') {
                    db = DBsystem();  // Reset database
                    cout << GREEN << "✓ Database cleared." << RESET << "\n";
                }
                break;
            case 13:
                displayStatistics(db);
                break;
            case 14:
                cout << "\n" << GREEN << "Goodbye! Database session ended." << RESET << "\n\n";
                return 0;
            default:
                cout << RED << "Invalid choice. Please try again." << RESET << "\n";
        }
    }

    return 0;
}
