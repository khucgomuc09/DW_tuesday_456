package staging;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.StringTokenizer;

import connectionDB.DBConnection;

public class ExtractFile {
	String jdbcURL_source, jdbcURL_dest, userName_source, userName_dest, pass_source, pass_dest, nameTable;

//	
	public ExtractFile() {
		// TODO Auto-generated constructor stub
	}

	public void staging() throws Exception {

		// 1. káº¿t ná»‘i xuá»‘ng table logs trong database control
		int id;
		Date time;
		String status, fileName, dir_local;
		Connection connectionControl = DBConnection.getConnectionControl();
		String sqlLogs = "select * from logs";
		PreparedStatement pre = connectionControl.prepareStatement(sqlLogs);
		ResultSet rs = pre.executeQuery();
		while (rs.next()) {
			id = rs.getInt("id");
			time = rs.getDate("time");
			fileName = rs.getNString("filename");
			System.out.println("name:" + fileName);
			status = rs.getNString("status");
			System.out.println("status: " + status);
			dir_local = rs.getNString("dir_local");

//		2. Kiá»ƒm tra file trong logs cÃ³ tá»“n táº¡i trong local hay k?
			String pathlocal = dir_local + "\\" + fileName;
			File fileLocal = new File(pathlocal);

			System.out.println("locaal" + fileLocal);
			if (!fileLocal.exists()) {
				System.out.println(pathlocal + " không tồn tại");

//			2.1Náº¿u khÃ´ng tá»“n táº¡i thÃ¬ set láº¡i trÆ°á»�ng status trong logs lÃ  "ERROR"
				String sqlSet = "Update logs set " + status + " = " + "'ERROR'," + time + " = NOW() " + "where id"
						+ " = " + id;
				pre = connectionControl.prepareStatement(sqlSet);
				pre.executeUpdate();
			} else {
				System.out.println("tồn tại");
				// status ok
				if (status.equals("OK")) {
					System.out.println("ok");
					// Ä‘á»�c name ra.xlsx convert load or load
					String lsFileName[] = fileName.split("\\.");
					System.out.println(lsFileName.toString());
					if (lsFileName[1].equals("txt") || lsFileName[1].equals("csv")) {
						System.out.println("txt " + fileName);
						ConvertTxtToCSV conTxt = new ConvertTxtToCSV();
						conTxt.convertFileTxtToCSV(fileLocal);
						load(pathlocal);

//						náº¿u lÃ  file xlsx thi convert sang file csv r thá»±c hiá»‡n load
					} else if (lsFileName[1].equals("xlsx")) {
						ConvertXLSXToCSV convert = new ConvertXLSXToCSV();
						try {
							convert.convertXLXSFileToCSV(fileLocal, 0);
						} catch (Exception e) {
							e.printStackTrace();
						}
						load(pathlocal);
						System.out.println("xlsx");
					}
				}
			}
		}
	}

	public void load(String excelFile) throws Exception {
		//
		// connect databaseControl
		Connection connectionControl = DBConnection.getConnectionControl();
		String sqlControl = "select * from myconfig";
		PreparedStatement preSource = connectionControl.prepareStatement(sqlControl);
		ResultSet rsSource = preSource.executeQuery();
		while (rsSource.next()) {
			nameTable = rsSource.getString("fileName");
			jdbcURL_source = rsSource.getString("source");
			userName_source = rsSource.getString("userName_source");
			pass_source = rsSource.getString("password_source");
		}
		System.out.println(nameTable);
		System.out.println(jdbcURL_source);
		System.out.println(userName_source);
		System.out.println(pass_source);

		// connect db source
		Connection connect = DBConnection.getConnection(jdbcURL_source, userName_source, pass_source);
		System.out.println("Connect DB Successfully :)");

//		Ä‘á»�c file 
//		tesst
//		File f = new File(excelFile);
//
//		ConvertXLSXToCSV co = new ConvertXLSXToCSV();
//		File news = new File("src\\week3\\17130008_sang_nhom15.csv");
////		try {
////			co.convertXLXSFileToCSV(f, 0);
////		} catch (Exception e) {
////			// TODO Auto-generated catch block
////			e.printStackTrace();
////		}

//		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
//				new FileOutputStream(excelFile.substring(0, excelFile.lastIndexOf(".")) + ".csv"), "UTF-8"));
//		out.write(co.convertXLXSFileToCSV(f, 0));
//		out.close();

		BufferedReader lineReader = new BufferedReader(new FileReader(excelFile));
//		System.out.println(f + "\tabc");
//		tesst
		System.out.println(excelFile + "\tex");
		String lineText = null;

		int count = 0;
		String sql;

		lineText = lineReader.readLine();
		System.out.println(lineText + "line");
		String[] fields = lineText.split(",");
		for (int i = 0; i < fields.length; i++) {
			fields[i] = fields[i].replace(" ", "_");
			System.out.println("cat fenmk" + fields[i]);
		}
		System.out.println(fields.length);
		System.out.println(lineText);
//		 create table 
		sql = "CREATE table " + nameTable + "(" + fields[0] + " CHAR(20)," + fields[1] + " CHAR(50)," + fields[2]
				+ " CHAR(50)," + fields[3] + " CHAR(50)," + fields[4] + " CHAR(50)," + fields[5] + " CHAR(50),"
				+ fields[6] + " CHAR(50)," + fields[7] + " CHAR(50)," + fields[8] + " CHAR(50)," + fields[9]
				+ " CHAR(50)," + fields[10] + " CHAR(50))";
		System.out.println(sql);
		PreparedStatement preparedStatement = connect.prepareStatement(sql);
		preparedStatement.execute();
		System.out.println("Create table Successfully :)");

		// skip header line
		String query = "INSERT INTO " + nameTable + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement pre = connect.prepareStatement(query);

//		lineText=lineReader.readLine();
		System.out.println(lineText + "sa");

//		String[] data = lineText.split("\\|");
//		System.out.println(data.length);
		while ((lineText = lineReader.readLine()) != null) {
			StringTokenizer tokenizer = new StringTokenizer(lineText, ",");
			String[] data = new String[11];
			int k = 0;
			while (tokenizer.hasMoreElements()) {
				data[k] = tokenizer.nextToken();
				k++;
			}
//			String[] data = lineText.split("\\|");
			System.out.println(data.length);
			for (int i = 0; i < data.length; i++) {
				System.out.print(data[i] + "\t\t");
			}
			System.out.println(data.toString());
			String id = data[0];
			System.out.println(id);
			String Mssv = data[1];
			System.out.println(Mssv);
			String ho = data[2];
			String ten = data[3];
			String dateOfBirth = data[4];
			String maLop = data[5];
			String tenLop = data[6];
			String sdt = data[7];
			String email = data[8];
			String queQuan = data[9];
			String note = data[10];
			pre.setString(1, id);
			pre.setString(2, Mssv);
			pre.setString(3, ho);
			pre.setString(4, ten);
			pre.setString(5, dateOfBirth);
			pre.setString(6, maLop);
			pre.setString(7, tenLop);
			pre.setString(8, sdt);
			pre.setString(9, email);
			pre.setString(10, queQuan);
			pre.setString(11, note);
			pre.execute();
		}
	}

	public void copy(String database1, String database2) throws ClassNotFoundException, SQLException {
		// ket noi toi databaseControl
		String nameDB = "";
		Connection connectionControl = DBConnection.getConnectionControl();
		String sqlControl = "select * from myconfig";
		PreparedStatement pre = connectionControl.prepareStatement(sqlControl);
		ResultSet resultSet = pre.executeQuery();
		while (resultSet.next()) {
			nameDB = resultSet.getString("dbname");
			jdbcURL_source = resultSet.getString("source");
			userName_source = resultSet.getString("userName_source");
			pass_source = resultSet.getString("password_source");
			jdbcURL_dest = resultSet.getString("dest");
			userName_dest = resultSet.getString("userName_dest");
			pass_dest = resultSet.getString("password_dest");
		}
		System.out.println(jdbcURL_source);
		System.out.println(userName_source);
		System.out.println(pass_source);
		System.out.println(jdbcURL_dest);
		System.out.println(userName_dest);
		System.out.println(pass_dest);

		// ket noi toi database chua file
		Connection connectionDB1 = DBConnection.getConnection(jdbcURL_source, userName_source, pass_source);
		System.out.println("c1 ok");
		Connection connectionDB2 = DBConnection.getConnection(jdbcURL_dest, userName_dest, pass_dest);
		System.out.println("c2 ok");

		ResultSet rs;
		Statement stmt = connectionDB1.createStatement();
		rs = stmt.executeQuery("SELECT * FROM " + nameDB);
		ResultSetMetaData md = (ResultSetMetaData) rs.getMetaData();
		int counter = md.getColumnCount();
		String colName[] = new String[counter];
		System.out.println("The column names are as follows:");
		for (int loop = 1; loop <= counter; loop++) {
			colName[loop - 1] = md.getColumnLabel(loop);
//			sqlCreateTable += colName[loop - 1] + " CHAR(50),";
		}
		String sqlCreateTable = "CREATE table " + nameDB + "copy" + "(" + colName[0] + " VARCHAR(15)," + colName[1]
				+ " CHAR(50)," + colName[2] + " CHAR(50)," + colName[3] + " CHAR(50)," + colName[4] + " CHAR(50),"
				+ colName[5] + " CHAR(50))";
//		sqlCreateTable += ")";

		System.out.println(sqlCreateTable);
		PreparedStatement p = connectionDB2.prepareStatement(sqlCreateTable);
		p.execute();

//		COPY 
		String insert = "INSERT INTO " + database2 + "." + nameDB + "copy " + "SELECT * FROM " + database1 + "."
				+ nameDB;
		System.out.println(insert);
		PreparedStatement pc = connectionDB2.prepareStatement(insert);
		pc.execute();
	}
}
