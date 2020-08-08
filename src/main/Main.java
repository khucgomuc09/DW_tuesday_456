package main;

import java.io.IOException;
import java.sql.SQLException;

import downloadFromSever.DownloadFileToLocal;
import staging.ExtractFile;

public class Main {

	public static void main(String[] args) throws Exception {
<<<<<<< HEAD
		int id = 1;
=======
		int id = 0;
>>>>>>> a904260f942f7b5ccea84d992df994921322ccf9
		for (int i = 0; i < args.length; i++) {
			id = Integer.parseInt(args[i]);
			System.out.println(args[i]);
		}
		DownloadFileToLocal.downloadFile(id + "");
//		try {
//
		ExtractFile.staging(id);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}

	}

}
