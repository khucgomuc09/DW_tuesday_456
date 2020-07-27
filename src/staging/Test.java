package staging;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class Test {
	public void w(String name) {
		File f = new File(name);
		File news = new File("");
//		BufferedWriter out= new BufferedWriter(new Ou)
	}

	public static void main(String[] args) throws Exception, SQLException, IOException {
//		String file = "src\\staging\\17130044_sang_nhom8.txt";
//		String file1 = "src\\staging\\sinhvien_chieu_nhom4.txt";
//		String file = "src\\staging\\17130008_sang_nhom15.xlsx";
		ExtractFile extractFile = new ExtractFile();

//		extractFile.loadTxtCsv(file);
//		extractFile.loadTxtCsv(file1);
//		extractFile.loadXlsx(file);
		extractFile.staging();
//		extractFile.copy("datawarehouse", "datacopy");

	}

}
