package staging;

import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

//import com.mysql.cj.result.Row;

public class ConvertXLSXToCSV {
	public String convertXLXSFileToCSV(File xlsxFile, int sheetIdx) throws Exception {
		StringBuffer sb = null;
		FileInputStream fileInStream = new FileInputStream(xlsxFile);

		// Open the xlsx and get the requested sheet from the workbook
		XSSFWorkbook workBook = new XSSFWorkbook(fileInStream);
		XSSFSheet selSheet = workBook.getSheetAt(sheetIdx);

		// Iterate through all the rows in the selected sheet
		Iterator<Row> rowIterator = selSheet.iterator();
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();

			// Iterate through all the columns in the row and build ","
			// separated string
			Iterator<Cell> cellIterator = row.cellIterator();
			sb = new StringBuffer();
			while (cellIterator.hasNext()) {
				Cell cell = cellIterator.next();
				if (sb.length() != 0) {
					sb.append(",");
				}
				// If you are using poi 4.0 or over, change it to
				// cell.getCellType
				switch (cell.getCellType()) {
				case STRING:
					sb.append(cell.getStringCellValue());
					break;
				case NUMERIC:
					sb.append((int) cell.getNumericCellValue());
					break;
				case BOOLEAN:
					sb.append(cell.getBooleanCellValue());
					break;
				default:
					sb.append(cell.getStringCellValue());
				}
			}
			System.out.println(sb.toString());
		}
		workBook.close();
		return sb.toString();
	}

	public static void main(String[] args) throws Exception {
		ConvertXLSXToCSV c = new ConvertXLSXToCSV();
		String abc = "src\\week3\\17130008_sang_nhom15.xlsx";
		File f = new File(abc);
		c.convertXLXSFileToCSV(f, 0);
	}
}
