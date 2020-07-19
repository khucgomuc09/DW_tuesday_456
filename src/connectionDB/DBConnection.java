package connectionDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
	private static Connection connect=null;
	public static Connection getConnectionControl() throws SQLException, ClassNotFoundException {
		String url = "jdbc:mysql://localhost/databasecontrol?"
				+ "useUnicode=true&useJDBCCompliantTimezoneShift=true&"
				+ "useLegacyDatetimeCode=false&serverTimezone=UTC&characterEncoding=UTF-8";
		String username = "root";
		String password = "";
	        if (connect == null|| connect.isClosed()) {
	            try {
	                Class.forName("com.mysql.cj.jdbc.Driver");//com.mysql.jdbc.Driver");
	                connect = DriverManager.getConnection(url,username,password);
	            } catch (ClassNotFoundException | SQLException e) {
	                e.printStackTrace();
	            }
	        }
	        return connect;
	    }
	public static Connection getConnection(String jdbcURL, String userName, String password)
			throws ClassNotFoundException, SQLException {
		Connection connection = DriverManager.getConnection(jdbcURL, userName, password);
		System.out.println("success");
		return connection;
	}
}