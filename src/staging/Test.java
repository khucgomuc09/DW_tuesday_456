package staging;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class Test {
	public void w(String name) {
		File f= new File(name);
		File news= new File("");
//		BufferedWriter out= new BufferedWriter(new Ou)
	}
	public static void main(String[] args) throws Exception, SQLException, IOException {
		String file = "src\\staging\\17130044_sang_nhom8.txt";
		ExtractFile extractFile = new ExtractFile();
		extractFile.load(file);
//		extractFile.staging();
//		extractFile.copy("datawarehouse", "datacopy");
	}
//	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
//		ExtractFile ex = new ExtractFile();
//		String urlFile = "src\\week3\\thongtincanhan.txt";
//		ex.load(urlFile);
//		ex.copy("datawarehouse", "datacopy");
//
//	}
}
 