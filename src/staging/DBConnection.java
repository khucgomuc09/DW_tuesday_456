package staging;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
	// connect database source and destination
	public static Connection getConnection(String jdbcURL, String userName, String password)
			throws ClassNotFoundException, SQLException {
		Connection connection = DriverManager.getConnection(jdbcURL, userName, password);
		System.out.println("success");
		return connection;
	}

	// Connect database control
	public static Connection getConnectionControl() throws SQLException {
		Connection connection = DriverManager.getConnection(
				"jdbc:mysql://localhost/databasecontrol?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&characterEncoding=UTF-8",
				"root", "");
		return connection;
	}

}
