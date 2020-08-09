package main;

import downloadFromSever.DownloadFileToLocal;
import staging.ExtractFile;

public class Main {

	public static void main(String[] args) throws Exception {
		ExtractFile ex;
		int id = 0;
		for (int i = 0; i < args.length; i++) {
			ex = new ExtractFile();
			id = Integer.parseInt(args[i]);
			System.out.println("idConfig: " + id);
			DownloadFileToLocal.downloadFile(id + "");
			ex.staging(id);
		}
	}

}
