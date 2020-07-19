package staging;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.mysql.cj.jdbc.DatabaseMetaData;
//import com.mysql.cj.result.Row;

import connectionDB.DBConnection;

public class ExtractFile {
	static String jdbcURL_source, jdbcURL_dest, userName_source, userName_dest, pass_source, pass_dest, nameTable;
	static File error = new File("src//error//error.txt");
	static BufferedWriter out;
	static String field = null;
	static int countField = 0;

	public ExtractFile() {
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(error), Charset.forName("UTF-8")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	// 1.Connect to staging
	public static Connection connectStaging() {
		Connection connectionControl;
		Connection connect = null;
		try {
			connectionControl = DBConnection.getConnectionControl();
			String sqlControl = "select * from myconfig";
			PreparedStatement preSource = connectionControl.prepareStatement(sqlControl);
			ResultSet rsSource = preSource.executeQuery();
			while (rsSource.next()) {
				nameTable = rsSource.getString("table_Name_Staging");
				jdbcURL_source = rsSource.getString("url_Source");
				userName_source = rsSource.getString("username_Source");
				pass_source = rsSource.getString("password_Source");
				field = rsSource.getString("fields");
			}
			connect = DBConnection.getConnection(jdbcURL_source, userName_source, pass_source);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return connect;
	}

	// 2.Load file from local to staging
	public void staging() throws Exception {
//		0.kết nối xuống mycofig lấy field để tạo header table
		Connection connectionControl = DBConnection.getConnectionControl();
		Connection connect = connectStaging();

		String fields[] = field.split(",");
		for (int i = 0; i < fields.length; i++) {
			fields[i] = fields[i].replace(" ", "_");
			countField++;
		}

		// 1. Kiểm tra xem table đã tồn tại trong staging hay chưa?
		DatabaseMetaData checkTable = (DatabaseMetaData) connect.getMetaData();
		ResultSet tables = checkTable.getTables(null, null, nameTable, null);
		// 1.1. Nếu tồn tại thì thông báo Table exist
		if (tables.next()) {
			System.out.println("Table ex");
		} else {
			// 1.2. Table không tồn tại thì tạo table với tên staging
			createTableStudent();
		}
		// end check table

		// 1. kết nối xuống table logs trong database control
		int id;
		Date time_download, time_staging;
		String status_download, status_staging, fileName, dir_local;
//		Connection connectionControl = DBConnection.getConnectionControl();
		String sqlLogs = "select * from log  LIMIT 1";
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
						loadTxtCsv(pathlocal, countField);
//						2.2: load success set status_staging and now() is SUCCESS
						String sqlSetSuccess = "Update log set status_Staging = 'SUCCESS' , time_staging = NOW() "
								+ "where idLogs" + " = " + id;
						System.out.println(sqlSetSuccess);
						pre = connectionControl.prepareStatement(sqlSetSuccess);
						pre.executeUpdate();

					} else if (lsFileName[1].equals("csv")) {
						loadTxtCsv(pathlocal, countField);
//						2.2: load success set status_staging and now() is SUCCESS
						String sqlSetSuccess = "Update log set status_Staging = 'SUCCESS' , time_staging = NOW() "
								+ "where idLogs" + " = " + id;
						System.out.println(sqlSetSuccess);
						pre = connectionControl.prepareStatement(sqlSetSuccess);
						pre.executeUpdate();
//						nếu là file xlsx thi thực hiện loadXlsx
					} else if (lsFileName[1].equals("xlsx")) {
//						loadXlsx(pathlocal);
//						2.2: load success set status_staging and now() is SUCCESS
						String sqlSetSuccess = "Update log set status_Staging = 'SUCCESS' , time_staging = NOW() "
								+ "where idLogs" + " = " + id;
						System.out.println(sqlSetSuccess);
						pre = connectionControl.prepareStatement(sqlSetSuccess);
						pre.executeUpdate();
						System.out.println("xlsx");
					}
				} else if (!status_download.equals("ER")) {
					System.out.println("File not exx");
					String sqlSetERROR = "Update log set status_download = 'ERROR',status_Staging = 'ERROR' , time_staging = NOW() "
							+ "where idLogs" + " = " + id;
					System.out.println(sqlSetERROR);
					pre = connectionControl.prepareStatement(sqlSetERROR);
					pre.executeUpdate();
				}
			}
		}
	}

	// 3.Read file text and file csv
	public void loadTxtCsv(String nameFile, int number_column) throws Exception {
//		1.connect database staging
		Connection connect = connectStaging();
		System.out.println("Connect DB Successfully :)");

		BufferedReader lineReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(nameFile), Charset.forName("UTF-8")));
//		4.đọc bỏ qua dòng header
		String lineText = null;
		lineText = lineReader.readLine();
//		int countField = 0;
//
//		String fields[] = field.split(",");
//		for (int i = 0; i < fields.length; i++) {
//			fields[i] = fields[i].replace(" ", "_");
//			countField++;
//		}

		String list = "";
		while ((lineText = lineReader.readLine()) != null) {
			System.out.println(lineText);
			StringTokenizer tokenizer = new StringTokenizer(lineText, ",|");
			// ktra có bao nhiêu trường trên dòng
			if (tokenizer.countTokens() < countField) {
				String text = nameFile + " không đủ dữ liệu các trường";
				out.write(text + "\n");
				out.flush();
				System.out.println(nameFile + " không đủ dữ liệu các trường");
				continue;
			} else {
				list += "('";
				System.out.println(tokenizer.countTokens());
				while (tokenizer.hasMoreElements()) {
					if (tokenizer.countTokens() == 1) {
						System.out.println("voooooo");
						list += tokenizer.nextToken() + "'";
					} else {
						list += tokenizer.nextToken() + "','";
					}
				}
				list += "),";
				System.out.println("list student: " + list);
			}
		}
		list = list.substring(0, list.lastIndexOf(","));
		System.out.println(list);
		String query = "INSERT INTO " + nameTable + " VALUES " + list;
		System.out.println(query);
		PreparedStatement pre = connect.prepareStatement(query);
		pre.execute();
	}

	// 4.Read file excel
	public void loadXlsx(String nameFile, int number_column) throws IOException, SQLException, ClassNotFoundException {
		Connection connect = connectStaging();
		System.out.println("Connect DB Successfully :)");

		FileInputStream fileInStream = new FileInputStream(nameFile);
		// 9.2.1: Mở xlsx và lấy trang tính yêu cầu từ bảng tính
		XSSFWorkbook workBook = new XSSFWorkbook(fileInStream);
		XSSFSheet selSheet = workBook.getSheetAt(0);

		// 9.2.2: Lặp qua tất cả các hàng trong trang tính đã chọn
		Iterator<Row> rowIterator = selSheet.iterator();
		List<String> listStudents = new ArrayList<String>();

		String list = "";
		while (rowIterator.hasNext()) {
			int temp = 0;
			Row row = rowIterator.next();

			// 9.2.3: Lặp qua tất cả các cột trong hàng và xây dựng "," tách chuỗi

			Iterator<Cell> cellIterator = row.cellIterator();
			// System.out.println(" count " +selSheet.getRow(0).getLastCellNum());
			if (selSheet.getRow(0).getLastCellNum() == number_column) {
				list = "(Null,";
				// System.out.println("row " + row);
				// while (cellIterator.hasNext()) {
				while (temp < number_column) {
					temp++;
					if (cellIterator.hasNext()) {

						Cell cell = cellIterator.next();
						// System.out.println("cell " + cell);
						switch (cell.getCellType()) {

						case STRING:
							String value = "";
							value = cell.getStringCellValue().replaceAll("'", "");
							// System.out.println(value);
							list += "'" + value + "'";
							break;
						case NUMERIC:
							list += "'" + cell.getNumericCellValue() + "'";
							break;
						case BOOLEAN:
							list += "'" + cell.getBooleanCellValue() + "'";
							break;

						default:
							list += "Null";
							break;
						}
						if (cell.getColumnIndex() == number_column - 1) {
							// bỏ dấu phẩy cuối
						} else
							list += ",";
					} else
						list += "Null";
				}
				list += ")\n";
				listStudents.add(list);
//				System.out.println(listStudents.toString());
			}
		}
		// 9.2.4: Bỏ phần header
		listStudents.remove(0);
		// 9.2.5: Add tất cả sinh viên theo định dạng câu lệnh insert sql
		String sql_students = "";
		for (int i = 0; i < listStudents.size(); i++) {
			sql_students += listStudents.get(i) + ",";
		}
		sql_students = sql_students.substring(0, sql_students.lastIndexOf(","));

		System.out.println(sql_students);
		// 9.2.6: Đóng file
		workBook.close();
		System.out.println("List ST: " + sql_students);
		String query = "INSERT INTO " + nameTable + " VALUES" + sql_students;
		System.out.println(query);
		PreparedStatement pre = connect.prepareStatement(query);
		pre.execute();
		System.out.println("excel file");
	}

	// 5.Convert date to yyyy-mm-dd
	public static String parseDate(String date) {
		if (date != null && !date.isEmpty()) {
			SimpleDateFormat format[] = new SimpleDateFormat[] { new SimpleDateFormat("dd/MM/yyyy"),
					new SimpleDateFormat("dd-MM-yyyy"), new SimpleDateFormat("MM/dd/yyyy"),
					new SimpleDateFormat("MM-dd-yyyy") };
			SimpleDateFormat result = new SimpleDateFormat("yyyy-MM-dd");
			Date parsedDate = null;
			for (int i = 0; i < format.length; i++) {
				try {
					format[i].setLenient(false);
					parsedDate = format[i].parse(date);
					return result.format(parsedDate);
				} catch (ParseException e) {
					continue;
				}
			}

		}
		return date;
	}

	// 6.Create table student
	public static void createTableStudent() throws ClassNotFoundException, SQLException {
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

		// load fields
		String f = "";
		String primary = fields[0];
		for (int i = 1; i < fields.length; i++) {
			if (i == fields.length - 1) {
				f += fields[i] + " CHAR(50)";
			} else {
				f += fields[i] + " CHAR(50),";
			}
		}

//		String sql = "CREATE table " + nameTable + "(" + fields[0] + " INT NOT NULL AUTO_INCREMENT ," + fields[1]
//				+ " CHAR(50)," + fields[2] + " CHAR(50)," + fields[3] + " CHAR(50)," + fields[4] + " CHAR(50),"
//				+ fields[5] + " CHAR(50)," + fields[6] + " CHAR(50)," + fields[7] + " CHAR(50)," + fields[8]
//				+ " CHAR(50)," + fields[9] + " CHAR(50)," + fields[10] + " CHAR(50),  PRIMARY KEY (" + fields[0] + "))";
		String sql = "CREATE table " + nameTable + "(" + fields[0] + " INT NOT NULL AUTO_INCREMENT ," + f
				+ ",  PRIMARY KEY (" + primary + "))";
		System.out.println(sql);
		PreparedStatement preparedStatement = connect.prepareStatement(sql);
		preparedStatement.execute();
	}

	public static void main(String[] args) {
//		String date = "15-08-1999";
//		System.out.println(date);
//		System.out.println(parseDate(date));
		String file = "src\\staging\\2003.xlsx";
		ExtractFile dm = new ExtractFile();
		try {
			dm.loadXlsx(file, 6);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
