package staging;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConvertTxtToCSV {
	BufferedReader read;
	BufferedWriter write;
	List<String> output = new ArrayList<String>();

	public void convertFileTxtToCSV(File text) throws IOException {
		read = new BufferedReader(new FileReader(text));
		String line;
		while ((line = read.readLine()) != null) {
			output.add(line.replaceAll("\\|", ","));
		}
		read.close();

//		write laij vafo file
		write = new BufferedWriter(new FileWriter(text));
		for (String s : output) {
			write.write(s);
			write.newLine();
		}
		write.flush();
		write.close();

		read.close();
		write.close();
	}

	public static void main(String[] args) {
		ConvertTxtToCSV mc = new ConvertTxtToCSV();
		File file = new File("src\\\\week3\\\\17130044_sang_nhom5.txt");
		try {
			mc.convertFileTxtToCSV(file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
