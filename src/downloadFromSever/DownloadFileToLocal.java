package downloadFromSever;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.apache.poi.ss.formula.functions.Now;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSocket;
import com.chilkatsoft.CkSsh;
import connectionDB.DBConnection;

public class DownloadFileToLocal {
	static {
		try {
			System.loadLibrary("chilkat");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public static boolean downloadFile() throws SQLException, ClassNotFoundException {
		// unlock ckglobal
		CkGlobal glob = new CkGlobal();
		boolean globsuccess = glob.UnlockBundle("unlocked");
		if (globsuccess != true) {
			System.out.println(glob.lastErrorText());
			return false;
		}
		int status = glob.get_UnlockStatus();
		if (status == 2) {
			System.out.println("Unlocked using purchased unlock code.");
		} else {
			System.out.println("Unlocked in trial mode.");
		}
		Connection conn = DBConnection.getConnectionControl();
		String sql = "select severaddress,usernamesever," + "passwordsever,portsever,remotedirsever,"
				+ "localdir,filenameconfig,extension from myconfig";
		PreparedStatement pre = conn.prepareStatement(sql);
		ResultSet rs = pre.executeQuery();
		rs.isBeforeFirst();
		//
		while (rs.next()) {
			CkSsh ssh = new CkSsh();
			CkSocket cksocket = new CkSocket();
			System.out.println(cksocket.lastErrorText());
			// Connect to an SSH server:
			String hostname = rs.getString("severaddress"); // drive.ecepvn.org
			int port = rs.getInt("portsever");// 2227
			boolean success = ssh.Connect(hostname, port);
			if (success != true) {
				System.out.println(ssh.lastErrorText());
				return false;
			}
			// Wait a max of 5 seconds when reading responses..
			ssh.put_IdleTimeoutMs(5000);
			String usernamesever = rs.getString("usernamesever");// "guest_access";
			String passSever = rs.getString("passwordsever");// "123456";
			// Authenticate using login/password:
			success = ssh.AuthenticatePw(usernamesever, passSever);
			if (success != true) {
				System.out.println(ssh.lastErrorText());
				return false;
			}
			CkScp scp = new CkScp();
			success = scp.UseSsh(ssh);
			if (success != true) {
				System.out.println(scp.lastErrorText());
				return false;
			}
			scp.put_HeartbeatMs(200);
			String filenameconf = rs.getString("filenameconfig");// + rs.getString(7);// "*.txt";
			String extension = rs.getString("extension");// + rs.getString(7);// "*.txt";
			scp.put_SyncMustMatch(filenameconf + "." + extension);
			String remoteDir = rs.getString("remotedirsever");// "/volume1/ECEP/song.nguyen/DW_2020/data";
			String localDir = rs.getString("localdir");// "C:\\Users\\Admin\\Downloads\\datawarehouse";
			int mode = 1;
			boolean bRecurse = false;
			success = scp.SyncTreeDownload(remoteDir, localDir, mode, bRecurse);
			
			if (success != true) {
				System.out.println(scp.lastErrorText());
				return false;
			}
			System.out.println("SCP download matching success.");
			// Disconnect
			ssh.Disconnect();
		}
		rs.close();
		pre.close();
		conn.close();
		return true;
	}

	public static void logs() throws ClassNotFoundException, SQLException {
		Connection conn = DBConnection.getConnectionControl();
		String sql = "select * from myconfig";
		PreparedStatement pre = conn.prepareStatement(sql);
		ResultSet rs = null;
		rs = pre.executeQuery();
		rs.beforeFirst();
		while (rs.next()) {
			String pathFolder = rs.getString("localDir");
			File folder = new File(pathFolder);
			if (!folder.exists()) {
				System.out.println("folder not exists!");
			} else {
				File[] listfile = folder.listFiles();
				int idconf = rs.getInt("id");
				String timedownload = "now()";
				String dirlocal = rs.getString("localDir");
				String status = "ER";
				for (File file : listfile) {
					String filename = file.getName();
					String sqlInsert = "insert into log values(null," + idconf + "," + "'" + filename + "'" + ","
							+ timedownload + "," + "'" + dirlocal + "'" + "," + "'" + status + "'" 
							+ "," + timedownload +",null,null"+")";
					System.out.println(sqlInsert);
					PreparedStatement preinsert = conn.prepareStatement(sqlInsert);
					preinsert.execute();
				}

			}
		}
	}

	public static void main(String argv[]) throws ClassNotFoundException, SQLException {
		downloadFile();
		logs();
	}
}