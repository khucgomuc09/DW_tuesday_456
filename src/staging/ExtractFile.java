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
	static int countField = 0, countLine = 0;

	public ExtractFile() {
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(error), Charset.forName("UTF-8")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	// 1.Connect to staging
	public static Connection connectStaging(int id) {
		Connection connectionControl;
		Connection connect = null;
		try {
			connectionControl = DBConnection.getConnectionControl();
			String sqlControl = "select * from myconfig where id=" + id;
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
	public static void staging(int idConfig) throws Exception {
//		0.kết nối xuống mycofig lấy field để tạo header table
		Connection connectionControl = DBConnection.getConnectionControl();
		Connection connect = connectStaging(idConfig);

		String fields[] = field.split(",");
		for (int i = 0; i < fields.length; i++) {
			fields[i] = fields[i].replace(" ", "_");
			countField++;
		}
		System.out.println(countField);

		// 1. kết nối xuống table logs trong database control
		int id;
		Date time_download, time_staging;
		String status, fileName, dir_local;
//		Connection connectionControl = DBConnection.getConnectionControl();
		String sqlLogs = "SELECT * FROM `log` JOIN myconfig ON log.idConfig=myconfig.id WHERE log.idConfig=" + idConfig;
		PreparedStatement pre = connectionControl.prepareStatement(sqlLogs);
		ResultSet rs = pre.executeQuery();
		while (rs.next()) {
			id = rs.getInt("idLogs");
			time_download = rs.getDate("time_Download");
			time_staging = rs.getDate("time_Staging");
			fileName = rs.getNString("file_Name");
			System.out.println("name:" + fileName);
			status = rs.getNString("status");
//			status_staging = rs.getNString("status_Staging");
			System.out.println("status: " + status);
			dir_local = rs.getNString("dir_Local");

//		2. Kiểm tra file trong logs có tồn tại trong local hay k?
			String pathlocal = dir_local + "\\" + fileName;
			File fileLocal = new File(pathlocal);

			System.out.println("locaal" + fileLocal);
			if (!fileLocal.exists()) {
				System.out.println(pathlocal + " không tồn tại");

//			2.1Nếu không tồn tại thì set lại trường status trong logs là "ERROR"
				String sqlSetERROR = "Update log set status= 'ERROR' , time_staging = NOW() " + " where idLogs" + " = "
						+ id;
				System.out.println(sqlSetERROR);
				pre = connectionControl.prepareStatement(sqlSetERROR);
				pre.executeUpdate();
			} else {
				System.out.println("tồn taji");
				// status ok
				if (status.equals("ER")) {
					System.out.println("ok");
					// đọc name ra.xlsx convert load or load
					String lsFileName[] = fileName.split("\\.");
					System.out.println(lsFileName.toString());
					if (lsFileName[1].equals("txt")) {
						System.out.println("txt " + fileName);
						ConvertTxtToCSV conTxt = new ConvertTxtToCSV();
						conTxt.convertFileTxtToCSV(fileLocal);
//						loadTxtCsv(pathlocal, countField);
						String list = loadTxtCsv(pathlocal, countField);
						if (list != "") {
							System.out.println(list);
							String query = "INSERT INTO " + nameTable + " VALUES " + list;
							System.out.println(query);
							PreparedStatement insert = connect.prepareStatement(query);
							countLine += insert.executeUpdate();
//						2.2: load success set status_staging and now() is SUCCESS
							String sqlSetSuccess = "Update log set status = 'SUCCESS' , time_staging = NOW(),number_row_success= "
									+ countLine + " where idLogs" + " = " + id;
							System.out.println(sqlSetSuccess);
							pre = connectionControl.prepareStatement(sqlSetSuccess);
							pre.executeUpdate();
						} else {
							System.out.println("File không có dữ liệu đúng");
						}

					} else if (lsFileName[1].equals("csv")) {
						String list = loadTxtCsv(pathlocal, countField);
						System.out.println(list);
						if (list != "") {
							String query = "INSERT INTO " + nameTable + " VALUES " + list;
							System.out.println(query);
							PreparedStatement insert = connect.prepareStatement(query);
							countLine += insert.executeUpdate();
//						2.2: load success set status_staging and now() is SUCCESS
							String sqlSetSuccess = "Update log set status = 'SUCCESS' , time_staging = NOW() "
									+ "where idLogs" + " = " + id;
							System.out.println(sqlSetSuccess);
							pre = connectionControl.prepareStatement(sqlSetSuccess);
							pre.executeUpdate();
						} else {
							System.out.println("File không có dữ liệu đúng");
						}
//						nếu là file xlsx thi thực hiện loadXlsx
					} else if (lsFileName[1].equals("xlsx")) {
//						
						String list = loadXlsx(pathlocal, countField);
						System.out.println(list);
						String query = "INSERT INTO " + nameTable + " VALUES " + list;
						System.out.println(query);
						PreparedStatement insert = connect.prepareStatement(query);
						countLine += insert.executeUpdate();
//						2.2: load success set status_staging and now() is SUCCESS
						String sqlSetSuccess = "Update log set status = 'SUCCESS' , time_staging = NOW(),number_row_success= "
								+ countLine + " where idLogs " + " = " + id;
						System.out.println(sqlSetSuccess);
						pre = connectionControl.prepareStatement(sqlSetSuccess);
						pre.executeUpdate();
						System.out.println("xlsx");
					}
					if (countLine <= 0) {
						String sqlSetSuccess = "Update log set status = 'ERROR' , time_staging = NOW(),number_row_success= "
								+ countLine + " where idLogs " + " = " + id;
						pre = connectionControl.prepareStatement(sqlSetSuccess);
						pre.executeUpdate();
					}
				} else if (status.equals("ERROR")) {
					System.out.println("File not exx");
					String sqlSetERROR = "Update log set status= 'ERROR' , time_staging = NOW() " + "where idLogs"
							+ " = " + id;
					System.out.println(sqlSetERROR);
					pre = connectionControl.prepareStatement(sqlSetERROR);
					pre.executeUpdate();
				}
			}
		}
	}

	// 3.Đọc file text và file csv
	public static String loadTxtCsv(String nameFile, int number_column) throws Exception {
		// 1.Đọc file theo từng dòng
		BufferedReader lineReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(nameFile), Charset.forName("UTF-8")));
		String lineText = null;
		// 2.Bỏ phần header
		lineText = lineReader.readLine();
		// 3.Đếm số dòng đủ dữ liệu các trường
//		countLine = 0;
		int sss = 0;

		String list = "";
		// 4.Trong khi dữ liệu không bằng null
		System.out.println(countField);
		while ((lineText = lineReader.readLine()) != null) {
//			System.out.println(lineText);
			// 4.1. Cắt chuỗi theo dấu , hoặc |
			StringTokenizer tokenizer = new StringTokenizer(lineText, ",|");
			// 4.2. Kiểm tra chuỗi vừa cắt có đủ dữ liệu các trường
			if (tokenizer.countTokens() < (number_column - 1)) {
				// 4.2.1. Nếu không đủ thì thông báo k đủ dữ liệu
//				String text = nameFile + " không đủ dữ liệu các trường";
//				out.write(text + "\n");
//				out.flush();
				System.out.println(nameFile + " không đủ dữ liệu các trường");
				continue;
			} else {
				// 4.2.2. Nếu đủ thì tăng countLine
				sss++;
//				countLine++;
				list += "(null,'";
//				System.out.println(tokenizer.countTokens());
				while (tokenizer.hasMoreElements()) {
					// 4.2.3.Nếu tổng số tokenizer bằng 1
					if (tokenizer.countTokens() == 1) {
//						System.out.println("voooooo");
						list += tokenizer.nextToken() + "'";
					} else {
						list += tokenizer.nextToken() + "','";
					}
				}
				list += "),";
				System.out.println("list student: " + list);
			}
		}
		// 6.Bỏ dấu phẩy ở cuối
		if (sss > 0)
			list = list.substring(0, list.lastIndexOf(","));
		else
			System.out.println("Tất cả dòng trong file đều sai");
		System.out.println(list);

		return list;
	}

	// 4.Đọc file excel
	public static String loadXlsx(String nameFile, int number_column)
			throws IOException, SQLException, ClassNotFoundException {
		// 1. Đọc nội dung file
		FileInputStream fileInStream = new FileInputStream(nameFile);
		// 2. Mở xlsx và lấy trang tính yêu cầu từ bảng tính
		XSSFWorkbook workBook = new XSSFWorkbook(fileInStream);
		// 3. Lấy sheet thứ nhất
		XSSFSheet selSheet = workBook.getSheetAt(0);

		// 4: Lặp qua tất cả các hàng trong trang tính đã chọn
		Iterator<Row> rowIterator = selSheet.iterator();
		// 5: Tạo list chứa tất cả các student
		List<String> list = new ArrayList<String>();

		String listItem = "";
		while (rowIterator.hasNext()) {
			int temp = 0;
			Row row = rowIterator.next();
			// 9.2.3: Lặp qua tất cả các cột trong hàng và xây dựng "," tách chuỗi
			Iterator<Cell> cellIterator = row.cellIterator();
			Cell cell = null;
			if (selSheet.getRow(0).getLastCellNum() == (number_column - 1)) {
				listItem = "(Null,";
				// while (cellIterator.hasNext()) {
				while (temp < number_column - 1) {
					temp++;
					if (cellIterator.hasNext()) {

						cell = cellIterator.next();
						switch (cell.getCellType()) {

						case STRING:
							String value = "";
							value = cell.getStringCellValue().replaceAll("'", "");
							listItem += "'" + value + "'";
							break;
						case NUMERIC:
							listItem += "'" + cell.getNumericCellValue() + "'";
							break;
						case BOOLEAN:
							listItem += "'" + cell.getBooleanCellValue() + "'";
							break;
						case _NONE:
							listItem += "'null'";
							break;
						case BLANK:
							listItem += "'null'";
							break;
						case ERROR:
							listItem += "'null'";
							break;
						case FORMULA:
							listItem += "'null'";
							break;

						default:
							listItem += "'null'";
							break;
						}
						if (cell.getColumnIndex() == number_column - 2) {
							// bỏ dấu phẩy cuối
////							list = list.substring(0, list.lastIndexOf(","));
							listItem += "";
						} else
							listItem += ",";
					} else
						listItem += "'null'";
				}
				listItem += ")\n";
				System.out.println(listItem);
				list.add(listItem);
			}
		}
		// 9.2.4: Bỏ phần header
		list.remove(0);

		// 9.2.5: Add tất cả sinh viên theo định dạng câu lệnh insert sql
		String sqlList = "";
		for (int i = 0; i < list.size(); i++) {
			String[] arr = list.get(i).split(",");
			if (arr[1].contains("'null'")) {
				list.remove(list.get(i));
				break;
			}
			sqlList += list.get(i) + ",";
		}
		sqlList = sqlList.substring(0, sqlList.lastIndexOf(","));

		System.out.println(sqlList);
		// 9.2.6: Đóng file
		workBook.close();
//		System.out.println("List ST: " + sql_students);
		return sqlList;
	}
}
