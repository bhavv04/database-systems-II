import java.io.*;
import java.sql.*;
import java.util.Scanner;
import java.io.Console;

class calGPA {
    
    public static String readEntry(String prompt) {
        Scanner scanner = new Scanner(System.in);
        System.out.print(prompt);
        return scanner.nextLine();
    }
    
    //hide password input 
    public static String readPassword(String prompt) {
        Console console = System.console();
        if (console == null) {
            // Fallback if console not available (like in some IDEs)
            System.out.print(prompt);
            Scanner scanner = new Scanner(System.in);
            return scanner.nextLine();
        }
        char[] passwordChars = console.readPassword(prompt);
        return new String(passwordChars);
    }
    
    public static void main(String args[]) throws SQLException, IOException {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException x) {
            System.out.println("Driver could not be loaded.");
            return;
        }
        
        String dbacct, passwrd, name;
        char grade;
        int credit;
        
        dbacct = readEntry("Enter database account (username): ");
        passwrd = readPassword("Enter password: ");  
        
        // Connection string
        String connectionURL = "jdbc:oracle:thin:@localhost:1521:XE";
        Connection conn = DriverManager.getConnection(connectionURL, dbacct, passwrd);
        
        String stmt1 = "SELECT G.Grade, C.Credit_hours " +
                       "FROM STUDENT S, GRADE_REPORT G, SECTION SEC, COURSE C " +
                       "WHERE G.Student_number = S.Student_number AND " +
                       "G.Section_identifier = SEC.Section_identifier AND " +
                       "SEC.Course_number = C.Course_number AND S.Name = ?";
        
        PreparedStatement p = conn.prepareStatement(stmt1);
        
        name = readEntry("Please enter student name: ");
        p.clearParameters();
        p.setString(1, name);
        
        ResultSet r = p.executeQuery();
        
        double totalCredits = 0, totalPoints = 0;
        int courseCount = 0;
        
        while (r.next()) {
            grade = r.getString(1).charAt(0);
            credit = r.getInt(2);
            
            double points = 0;
            switch (grade) {
                case 'A': points = 4.0; break;
                case 'B': points = 3.0; break;
                case 'C': points = 2.0; break;
                case 'D': points = 1.0; break;
                case 'F': points = 0.0; break;
                default: 
                    System.out.println("This grade " + grade + " will not be calculated.");
                    continue;
            }
            
            totalPoints += (points * credit);
            totalCredits += credit;
            courseCount++;
        }
        
        if (courseCount > 0) {
            double avg = totalPoints / totalCredits;
            System.out.println("Student named " + name + " has a grade point average of " + 
                             String.format("%.2f", avg) + ".");
        } else {
            System.out.println("No grades found for student: " + name);
        }
        
        r.close();
        p.close();
        conn.close();
    }
}