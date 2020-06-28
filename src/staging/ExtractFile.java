package staging;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.StringTokenizer;

import connectionDB.DBConnection;

public class ExtractFile {
	String jdbcURL_source, jdbcURL_dest, userName_source, userName_dest, pass_source, pass_dest, nameTable;

	public void staging() throws Exception {
//		0.kết nối xuống mycofig lấy field để tạo header table
		Connection connectionControl = DBConnection.getConnectionControl();
		String sqlControl = "select * from myconfig";
		PreparedStatement preSource = connectionControl.prepareStatement(sqlControl);
		ResultSet rsSource = preSource.executeQuery();
		String field = null;
		while (rsSource.next()) {
			nameTable = rsSource.getString("table_Name_Staging");
			jdbcURL_source = rsSource.getString("url_Source");
			userName_source = rsSource.getString("username_Source");
			pass_source = rsSource.getString("password_Source");
			field = rsSource.getString("fields");
		}
		System.out.println(nameTable);
		System.out.println(jdbcURL_source);
		System.out.println(userName_source);
		System.out.println(pass_source);
		System.out.println(field);

		String fields[] = field.split(",");
		for (int i = 0; i < fields.length; i++) {
			fields[i] = fields[i].replace(" ", "_");
		}

		// bvconnect db staging create table
		Connection connect = DBConnection.getConnection(jdbcURL_source, userName_source, pass_source);
		System.out.println("Connect DB Successfully :)");

		String sql = "CREATE table " + nameTable + "(" + fields[0] + " INT NOT NULL AUTO_INCREMENT ," + fields[1]
				+ " CHAR(50)," + fields[2] + " CHAR(50)," + fields[3] + " CHAR(50)," + fields[4] + " CHAR(50),"
				+ fields[5] + " CHAR(50)," + fields[6] + " CHAR(50)," + fields[7] + " CHAR(50)," + fields[8]
				+ " CHAR(50)," + fields[9] + " CHAR(50)," + fields[10] + " CHAR(50),  PRIMARY KEY (" + fields[0] + "))";
		System.out.println(sql);
		PreparedStatement preparedStatement = connect.prepareStatement(sql);
		preparedStatement.execute();
		System.out.println("Create table Successfully :)");

		// 1. kết nối xuống table logs trong database control
		int id;
		Date time_download, time_staging;
		String status_download, status_staging, fileName, dir_local;
//		Connection connectionControl = DBConnection.getConnectionControl();
		String sqlLogs = "select * from log";
		PreparedStatement pre = connectionControl.prepareStatement(sqlLogs);
		ResultSet rs = pre.executeQuery();
		while (rs.next()) {
			id = rs.getInt("idLogs");
			time_download = rs.getDate("time_Download");
			time_staging = rs.getDate("time_Staging");
			fileName = rs.getNString("file_Name");
			System.out.println("name:" + fileName);
			status_download = rs.getNString("status_Download");
			status_staging = rs.getNString("status_Staging");
			System.out.println("status: " + status_download);
			dir_local = rs.getNString("dir_Local");

//		2. Kiểm tra file trong logs có tồn tại trong local hay k?
			String pathlocal = dir_local + "\\" + fileName;
			File fileLocal = new File(pathlocal);

			System.out.println("locaal" + fileLocal);
			if (!fileLocal.exists()) {
				System.out.println(pathlocal + " không tồn tại");

//			2.1Nếu không tồn tại thì set lại trường status trong logs là "ERROR"
				String sqlSetERROR = "Update log set status_download = 'ERROR',status_Staging = 'ERROR' , time_staging = NOW() "
						+ "where idLogs" + " = " + id;
				System.out.println(sqlSetERROR);
				pre = connectionControl.prepareStatement(sqlSetERROR);
				pre.executeUpdate();
			} else {
				System.out.println("tồn taji");
				// status ok
				if (status_download.equals("ER")) {
					System.out.println("ok");
					// đọc name ra.xlsx convert load or load
					String lsFileName[] = fileName.split("\\.");
					System.out.println(lsFileName.toString());
					if (lsFileName[1].equals("txt")) {
						System.out.println("txt " + fileName);
						ConvertTxtToCSV conTxt = new ConvertTxtToCSV();
						conTxt.convertFileTxtToCSV(fileLocal);
						loadTxtCsv(pathlocal);
//						2.2: load success set status_staging and now() is SUCCESS
						String sqlSetSuccess = "Update log set status_Staging = 'SUCCESS' , time_staging = NOW() "
								+ "where idLogs" + " = " + id;
						System.out.println(sqlSetSuccess);
						pre = connectionControl.prepareStatement(sqlSetSuccess);
						pre.executeUpdate();

					} else if (lsFileName[1].equals("csv")) {
						loadTxtCsv(pathlocal);
//						2.2: load success set status_staging and now() is SUCCESS
						String sqlSetSuccess = "Update log set status_Staging = 'SUCCESS' , time_staging = NOW() "
								+ "where idLogs" + " = " + id;
						System.out.println(sqlSetSuccess);
						pre = connectionControl.prepareStatement(sqlSetSuccess);
						pre.executeUpdate();
//						nếu là file xlsx thi thực hiện loadXlsx
					} else if (lsFileName[1].equals("xlsx")) {
						loadXlsx(pathlocal);
//						2.2: load success set status_staging and now() is SUCCESS
						String sqlSetSuccess = "Update log set status_Staging = 'SUCCESS' , time_staging = NOW() "
								+ "where idLogs" + " = " + id;
						System.out.println(sqlSetSuccess);
						pre = connectionControl.prepareStatement(sqlSetSuccess);
						pre.executeUpdate();
						System.out.println("xlsx");
					}
				}
			}
		}
	}

	public void loadTxtCsv(String nameFile) throws Exception {

		Connection connectionControl = DBConnection.getConnectionControl();
		String sqlControl = "select * from myconfig";
		PreparedStatement preSource1 = connectionControl.prepareStatement(sqlControl);
		ResultSet rsSource1 = preSource1.executeQuery();
//		String field = null;
		while (rsSource1.next()) {
			nameTable = rsSource1.getString("table_Name_Staging");
			jdbcURL_source = rsSource1.getString("url_Source");
			userName_source = rsSource1.getString("username_Source");
			pass_source = rsSource1.getString("password_Source");
//			fieldsss = rsSource1.getString("fields");
		}

		Connection connect = DBConnection.getConnection(jdbcURL_source, userName_source, pass_source);
		System.out.println("Connect DB Successfully :)");

		BufferedReader lineReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(nameFile), Charset.forName("UTF-8")));
//		đọc bỏ qua dòng header
		String lineText = null;
		lineText = lineReader.readLine();

		// skip header line
		String query = "INSERT INTO " + nameTable
				+ "(Mã_sinh_viên,Họ_lót,Tên,Ngày_sinh,Mã_lớp,Tên_lớp,Đt_liên_lạc,Email,Quê_quán,Ghi_chú) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement pre = connect.prepareStatement(query);

//		lineText=lineReader.readLine();
		System.out.println(lineText + "sa");

//		String[] data = lineText.split("\\|");
//		System.out.println(data.length);
		while ((lineText = lineReader.readLine()) != null) {
			StringTokenizer tokenizer = new StringTokenizer(lineText, ",");
			String[] data = new String[11];
			String pattern = "yyyy-MM-dd";
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
//			data[4] = simpleDateFormat.format(new Date(data[4]));
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
			String Mssv = data[1];
			System.out.println(Mssv);
			String ho = data[2];
			String ten = data[3];
			String dateOfBirth = data[4];
			String maLop = data[5];
			String tenLop = data[6];
			String sđt = data[7];
			String email = data[8];
			String queQuan = data[9];
			String note = data[10];
			pre.setString(1, Mssv);
			pre.setString(2, ho);
			pre.setString(3, ten);
			pre.setString(4, dateOfBirth);
			pre.setString(5, maLop);
			pre.setString(6, tenLop);
			pre.setString(7, sđt);
			pre.setString(8, email);
			pre.setString(9, queQuan);
			pre.setString(10, note);
			pre.execute();
		}
	}

	public void loadXlsx(String nameFile) throws IOException, SQLException, ClassNotFoundException {

		// connect db source
		Connection connect = DBConnection.getConnection(jdbcURL_source, userName_source, pass_source);
		System.out.println("Connect DB Successfully :)");

		String query = "INSERT INTO " + nameTable + " VALUES(NULL,?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement pre = connect.prepareStatement(query);

		List<Student> listStudent = ReadFromExcelFile.readBooksFromExcelFile(nameFile);
		for (Student student : listStudent) {
			System.out.println(student.toString() + "student");

//		}
//		while ((lineText = lineReader.readLine()) != null) {
			StringTokenizer tokenizer = new StringTokenizer(student.toString(), ",");
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
			String Mssv = data[1];
			System.out.println(Mssv);
			String ho = data[2];
			String ten = data[3];
			String dateOfBirth = data[4];
			String maLop = data[5];
			String tenLop = data[6];
			String sđt = data[7];
			String email = data[8];
			String queQuan = data[9];
			String note = data[10];
			pre.setString(1, Mssv);
			pre.setString(2, ho);
			pre.setString(3, ten);
			pre.setString(4, dateOfBirth);
			pre.setString(5, maLop);
			pre.setString(6, tenLop);
			pre.setString(7, sđt);
			pre.setString(8, email);
			pre.setString(9, queQuan);
			pre.setString(10, note);
			pre.execute();
		}

		System.out.println("excel file");
	}
}
