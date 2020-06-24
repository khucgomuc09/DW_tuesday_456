package connectionDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
	private static Connection connect=null;
	public static Connection getConnection() throws SQLException, ClassNotFoundException {
		String url = "jdbc:mysql://localhost:3306/dbcontroler?useUnicode=true&characterEncoding=utf-8";
		String username = "root";
		String password = "";
	        if (connect == null|| connect.isClosed()) {
	            try {
	                Class.forName("com.mysql.jdbc.Driver");
	                connect = DriverManager.getConnection(url,username,password);
	            } catch (ClassNotFoundException | SQLException e) {
	                e.printStackTrace();
	            }
	        }
	        return connect;
	    }
}