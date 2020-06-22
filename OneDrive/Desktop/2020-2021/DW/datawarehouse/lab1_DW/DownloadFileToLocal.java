package lab1_DW;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
	public static void main(String argv[]) throws ClassNotFoundException, SQLException {
		// unlock ckglobal
		CkGlobal glob = new CkGlobal();
		boolean globsuccess = glob.UnlockBundle("unlocked");
		if (globsuccess != true) {
			System.out.println(glob.lastErrorText());
			return;
		}
		int status = glob.get_UnlockStatus();
		if (status == 2) {
			System.out.println("Unlocked using purchased unlock code.");
		} else {
			System.out.println("Unlocked in trial mode.");
		}
		Connection conn = DBConnection.getConnection();
		String sql = "select severaddress,usernamesever," + "passwordsever,portsever,remotedirsever,"
				+ "localdir,extension from myconfig";
		PreparedStatement pre = conn.prepareStatement(sql);
		ResultSet rs = pre.executeQuery();
		rs.isBeforeFirst();
		//
		while (rs.next()) {
			CkSsh ssh = new CkSsh();
			CkSocket cksocket = new CkSocket();
			System.out.println(cksocket.lastErrorText());
			// Connect to an SSH server:
			String hostname = rs.getString(1); // drive.ecepvn.org
			int port = rs.getInt(4);// 2227
			System.out.println(hostname);
			boolean success = ssh.Connect(hostname, port);
			if (success != true) {
				System.out.println(ssh.lastErrorText());
				return;
			}
			// Wait a max of 5 seconds when reading responses..
			ssh.put_IdleTimeoutMs(5000);
			String usernamesever = rs.getString(2);// "guest_access";
			String passSever = rs.getString(3);// "123456";
			// Authenticate using login/password:
			success = ssh.AuthenticatePw(usernamesever, passSever);
			if (success != true) {
				System.out.println(ssh.lastErrorText());
				return;
			}
			// Once the SSH object is connected and authenticated, use it
			// in the SCP object.
			CkScp scp = new CkScp();
			success = scp.UseSsh(ssh);
			if (success != true) {
				System.out.println(scp.lastErrorText());
				return;
			}
			scp.put_HeartbeatMs(200);
			String extension = "*.*" ;//+ rs.getString(7);// "*.txt";
			// Set the SyncMustMatch property to "*.pem" to download only .pem files
			scp.put_SyncMustMatch(extension);
			String remoteDir = rs.getString(5);// "/volume1/ECEP/song.nguyen/DW_2020/data";
			String localDir = rs.getString(6);// "C:\\Users\\Admin\\Downloads\\datawarehouse";
			System.out.println(localDir);
			int mode = 0;
			// Do not recurse the remote directory tree. Only download files matching *.pem
			// from the given remote directory.
			boolean bRecurse = false;
			success = scp.SyncTreeDownload(remoteDir, localDir, mode, bRecurse);
			if (success != true) {
				System.out.println(scp.lastErrorText());
				return;
			}
			System.out.println("SCP download matching success.");
			// Disconnect
			ssh.Disconnect();
		}
	}
}