package staging;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

public class testConvert {
	public static void convertExcelToCSV(String fileName) throws InvalidFormatException, IOException {

//		BufferedWriter output = new BufferedWriter(
//				new FileWriter(fileName.substring(0, fileName.lastIndexOf(".")) + ".csv"));
		BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(fileName.substring(0, fileName.lastIndexOf(".")) + ".csv"), "UTF-8"));

		InputStream is = new FileInputStream(fileName);

		Workbook wb = WorkbookFactory.create(is);

		Sheet sheet = wb.getSheetAt(0);

		// hopefully the first row is a header and has a full compliment of
		// cells, else you'll have to pass in a max (yuck)
		int maxColumns = sheet.getRow(0).getLastCellNum();

		for (Row row : sheet) {

			// row.getFirstCellNum() and row.getLastCellNum() don't return the
			// first and last index when the first or last column is blank
			int minCol = 0; // row.getFirstCellNum()
			int maxCol = maxColumns; // row.getLastCellNum()

			for (int i = minCol; i < maxCol; i++) {

				Cell cell = row.getCell(i);
				String buf = "";
				if (i > 0) {
					buf = ",";
				}

				if (cell == null) {
					output.write(buf);
					// System.out.print(buf);
				} else {

					String v = null;

					switch (cell.getCellType()) {
					case STRING:
						v = cell.getRichStringCellValue().getString();
						System.out.println("success");
						break;
					case NUMERIC:
						if (DateUtil.isCellDateFormatted(cell)) {
							v = cell.getDateCellValue().toString();
						} else {
							v = String.valueOf(cell.getNumericCellValue());
						}
						break;
					case BOOLEAN:
						v = String.valueOf(cell.getBooleanCellValue());
						break;
					case FORMULA:
						v = cell.getCellFormula();
						break;
					default:
					}

					if (v != null) {
						buf = buf + toCSV(v);
					}
					output.write(buf);
					// System.out.print(buf);
				}
			}

			output.write("\n");
			// System.out.println();
		}
		is.close();
		output.close();
	}

	/*
	 * </strong> Escape the given value for output to a CSV file. Assumes the value
	 * does not have a double quote wrapper.
	 * 
	 * @return
	 */
	public static String toCSV(String value) {

		String v = null;
		boolean doWrap = false;

		if (value != null) {

			v = value;

			if (v.contains("\"")) {
				v = v.replace("\"", "\"\""); // escape embedded double quotes
				doWrap = true;
			}

			if (v.contains(",") || v.contains("\n")) {
				doWrap = true;
			}

			if (doWrap) {
				v = "\"" + v + "\""; // wrap with double quotes to hide the comma
			}
		}

		return v;
	}

	public static void main(String[] args) throws InvalidFormatException, IOException {
		testConvert c = new testConvert();
		c.convertExcelToCSV("src\\week3\\17130008_sang_nhom15.xlsx");
	}
}
