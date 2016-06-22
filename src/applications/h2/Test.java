import java.sql.*;
public class Test {
    public static void main(String[] a)
            throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.
            getConnection("jdbc:h2:tcp://localhost/~/test", "sa", "");
        // add application code here

	Statement stmt = conn.createStatement();
	
	ResultSet rs = stmt.executeQuery("SELECT * FROM HAHA");
	System.out.println(rs);
        conn.close();
    }
}
