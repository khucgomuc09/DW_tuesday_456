package main;

import java.io.IOException;
import java.sql.SQLException;

import downloadFromSever.DownloadFileToLocal;
import staging.ExtractFile;

public class Main {

	public static void main(String[] args) throws Exception {
		int id = 1;
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
