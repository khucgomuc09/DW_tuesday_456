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
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import connectionDB.DBConnection;

public class ExtractFile {
	static String jdbcURL_source, jdbcURL_dest, userName_source, userName_dest, pass_source, pass_dest, nameTable;
	static File error = new File("src//error//error.txt");
	static BufferedWriter out;
	static String field = null;
	static int countField = 0, countLine = 0, id;
	static Date time_download;
	static Date time_staging;
	static String status, fileName, dir_local;

	public static Connection connectStaging(int id) {
		// 1. Kết nối databaseControll
		Connection connectionControl;
		Connection connect = null;
		try {
			connectionControl = DBConnection.getConnectionControl();
			// 2. Kết nối đến table myconfig
			String sqlControl = "select * from myconfig where id=" + id;
			PreparedStatement preSource = connectionControl.prepareStatement(sqlControl);
			ResultSet rsSource = preSource.executeQuery();
			// 3. Lấy dữ liệu trong resultset, Nếu có dữ liệu thì lấy dữ liệu các cột:
			// nameTable,jdbcURL_source,userName_source,pass_source,field.
			while (rsSource.next()) {
				nameTable = rsSource.getString("table_Name_Staging");
				jdbcURL_source = rsSource.getString("url_Source");
				userName_source = rsSource.getString("username_Source");
				pass_source = rsSource.getString("password_Source");
				field = rsSource.getString("fields");
			}
			// 4. Kết nối tới database Staging
			connect = DBConnection.getConnection(jdbcURL_source, userName_source, pass_source);

		} catch (ClassNotFoundException | SQLException e) {
			// Kết nối không thành công
			System.out.println("Kết nối không thành công");
			e.printStackTrace();
		}
		return connect;
	}

	// 2.Load file from local to staging
	public static void staging(int idConfig) throws Exception {
//		1. Kết nối tới databaseControll
		Connection connectionControl = DBConnection.getConnectionControl();

		// 4. Lấy dữ liệu các field, thực hiện cắt field bởi dấu phẩy, sử dụng biến
		// countField để đếm tổng số field.

		// 5. Kết nối tới database Staging
		Connection connect = connectStaging(idConfig);
		String fields[] = field.split(",");
		countField = 0;
		for (int i = 0; i < fields.length; i++) {
			fields[i] = fields[i].replace(" ", "_");
			countField++;
		}
		// 6. Thực hiên câu truy vấn JOIN table log và table config .
		String sqlLogs = "SELECT * FROM `log` JOIN myconfig ON log.idConfig=myconfig.id WHERE log.idConfig=" + idConfig;
		PreparedStatement pre = connectionControl.prepareStatement(sqlLogs);
		ResultSet rs = pre.executeQuery();
		// Lấy dữ liệu của các cột: id, time_download, time_staging, fileName, status,
		// dir_local.
		while (rs.next()) {
			id = rs.getInt("idLogs");
			time_download = rs.getDate("time_Download");
			time_staging = rs.getDate("time_Staging");
			fileName = rs.getNString("file_Name");
			status = rs.getNString("status");
			System.out.println("status: " + status);
			dir_local = rs.getNString("dir_Local");

			// 7. Kiểm tra file trong logs có tồn tại trong local hay k?
			String pathlocal = dir_local + "\\" + fileName;
			File fileLocal = new File(pathlocal);
			// Nếu file không tồn tại
			if (!fileLocal.exists()) {
				System.out.println(pathlocal + " không tồn tại");
				// 7.1: Update status là error, time_staging là thời giạn hiện tại
				String sqlSetERROR = "Update log set status= 'ERROR' , time_staging = NOW() " + " where idLogs" + " = "
						+ id;
				pre = connectionControl.prepareStatement(sqlSetERROR);
				pre.executeUpdate();
				// Nếu file tồn tại
			} else {
				System.out.println(pathlocal + " tồn tại");
				// status ok
				if (status.equals("ER")) {
					// 8: in ra Bắt đầu load file
					System.out.println("Bắt đầu load file");
					// 9: cắt tên file để lấy phần mở rộng
					String lsFileName[] = fileName.split("\\.");
					// 10. Nếu là file txt hoặc file csv
					if (lsFileName[1].equals("txt") || lsFileName[1].equals("csv")) {
						countLine = 0;
						// 10.1: Convert các kí tự đặc biệt(| \t) trong file về dấu ,
						ConvertTxtToCSV conTxt = new ConvertTxtToCSV();
						conTxt.convertFileTxtToCSV(fileLocal);
						// 10.2: Đọc file txt với số trường đếm được và lưu vào list
						String list = loadTxtCsv(pathlocal, countField);
						if (list != "") {
							// 10.3: insert vào nametable của database Staging với dữ liệu là list
							String query = "INSERT INTO " + nameTable + " VALUES " + list;
							System.out.println(query);
							PreparedStatement insert = connect.prepareStatement(query);
							// 10.4: Đếm số dòng insert được vào trong nametable
							countLine += insert.executeUpdate();
							// 10.5: Update status là SUCCESS, time_staging là thời giạn hiện
							// tại,number_row_success là số dòng insert được vào nametable
							String sqlSetSuccess = "Update log set status = 'SUCCESS' , time_staging = NOW(),number_row_success= "
									+ countLine + " where idLogs" + " = " + id;
							System.out.println("Loaf file " + fileName + " thành công với " + countLine + " dòng");
							System.out.println("----------------------------------------------------");
							pre = connectionControl.prepareStatement(sqlSetSuccess);
							pre.executeUpdate();
							// Nếu file không insert được hàng nào vào staging
							if (countLine <= 0) {
								System.out.println("File không có dữ liệu đúng");
								System.out.println("----------------------------------------------------");
								// 10.6: Update status là error, time_staging là thời giạn hiện tại
								String sqlSetERROR = "Update log set status= 'ERROR' , time_staging = NOW() "
										+ "where idLogs" + " = " + id;
								pre = connectionControl.prepareStatement(sqlSetERROR);
								pre.executeUpdate();
							}
						}
						// 11. Nếu là file xlsx
					} else if (lsFileName[1].equals("xlsx")) {
						countLine = 0;
						System.out.println(countField);
						// 11.1: Đọc file txt với số trường đếm được và lưu vào list
						String list = loadXlsx(pathlocal, countField);
//						System.out.println(list);
						if (list != "") {
							// 10.3: insert vào nametable với dữ liệu là list
							String query = "INSERT INTO " + nameTable + " VALUES " + list;
							System.out.println(query);
							PreparedStatement insert = connect.prepareStatement(query);
							// 10.4: Đếm số dòng insert được vào trong nametable
							countLine += insert.executeUpdate();
							// 10.5: Update status là SUCCESS, time_staging là thời giạn hiện
							// tại,number_row_success là số dòng insert được vào nametable
							String sqlSetSuccess = "Update log set status = 'SUCCESS' , time_staging = NOW(),number_row_success= "
									+ countLine + " where idLogs " + " = " + id;
							System.out.println("Loaf file " + fileName + " thành công với " + countLine + " dòng");
							pre = connectionControl.prepareStatement(sqlSetSuccess);
							pre.executeUpdate();
							if (countLine <= 0) {
								System.out.println("File không có dữ liệu đúng");
								System.out.println("----------------------------------------------------");
								// 10.6: Update status là error, time_staging là thời giạn hiện tại
								String sqlSetERROR = "Update log set status= 'ERROR' , time_staging = NOW() "
										+ "where idLogs" + " = " + id;
								pre = connectionControl.prepareStatement(sqlSetERROR);
								pre.executeUpdate();
							}
						}
					} else if (!(lsFileName[1].equals("txt")) || !(lsFileName[1].equals("csv"))
							|| !(lsFileName[1].equals("xlsx"))) {
						System.out.println("Không thể load được file");
						System.out.println("----------------------------------------------------");
						// 12: Update status là error, time_staging là thời giạn hiện tại
						String sqlSetERROR = "Update log set status= 'ERROR' , time_staging = NOW() " + "where idLogs"
								+ " = " + id;
						pre = connectionControl.prepareStatement(sqlSetERROR);
						pre.executeUpdate();
					}
					// Nếu status là ERROR
				} else if (status.equals("ERROR")) {
					System.out.println("File chưa sẵn sàng để load vào staging");
					System.out.println("----------------------------------------------------");
					// 7.2: Update status là error, time_staging là thời giạn hiện tại
					String sqlSetERROR = "Update log set status= 'ERROR' , time_staging = NOW() " + "where idLogs"
							+ " = " + id;
					pre = connectionControl.prepareStatement(sqlSetERROR);
					pre.executeUpdate();
					// Nếu status là SUCCESS
				} else if (status.equals("SUCCESS")) {
					// 7.3: in ra File đã load vào staging
					System.out.println("File đã load vào staging");
					System.out.println("----------------------------------------------------");
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
		int count = 0;

		String list = "";
		// 4.Trong khi dữ liệu không bằng null
//		System.out.println(countField);
		while ((lineText = lineReader.readLine()) != null) {
			// 4.1. Cắt chuỗi theo dấu , hoặc |
			StringTokenizer tokenizer = new StringTokenizer(lineText, ",|");
			// 4.2. Kiểm tra chuỗi vừa cắt có đủ dữ liệu các trường
			if (tokenizer.countTokens() < (number_column - 1)) {
				// 4.2.1. Nếu không đủ thì tiếp tục chay dòng khác
				continue;
			} else {
				// 4.2.2. Nếu đủ thì tăng countLine
				count++;
				list += "(null,'";
				while (tokenizer.hasMoreElements()) {
					// 4.2.3.Nếu tổng số tokenizer bằng 1
					if (tokenizer.countTokens() == 1) {
						list += tokenizer.nextToken() + "'";
					} else {
						list += tokenizer.nextToken() + "','";
					}
				}
				list += "),";
			}
		}
		// 6.Bỏ dấu phẩy ở cuối
		if (count > 0)
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
		// 5: Tạo list chứa tất cả các dòng
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
//						case _NONE:
//							listItem += "'null'";
//							break;
//						case BLANK:
//							listItem += "'null'";
//							break;
//						case ERROR:
//							listItem += "'null'";
//							break;
//						case FORMULA:
//							listItem += "'null'";
//							break;

						default:
							listItem += "'null'";
							break;
						}
						if (cell.getColumnIndex() == number_column - 2) {
							// bỏ dấu phẩy cuối
							listItem += "";
						} else
							listItem += ",";
					} else
						listItem += "'null'";
				}
				listItem += ")\n";
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

		// 9.2.6: Đóng file
		workBook.close();
		return sqlList;
	}
}
