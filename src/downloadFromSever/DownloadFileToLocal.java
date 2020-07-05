package downloadFromSever;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Transport;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSocket;
import com.chilkatsoft.CkSsh;
//import com.mysql.cj.Session;

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

	public static boolean downloadFile() throws SQLException, ClassNotFoundException, IOException {
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
//		String sql = "select sever_Address,username_Sever," + "password_Sever,port_Sever,remote_DirSever,"
//				+ "local_Dir,file_Name_Config,extension from myconfig";
		String sql ="select * from myconfig";
		PreparedStatement pre = conn.prepareStatement(sql);
		ResultSet rs = pre.executeQuery();
		rs.isBeforeFirst();
		//
		while (rs.next()) {
			String mail_Recieved=rs.getString("mail_Recieved");
			CkSsh ssh = new CkSsh();
			CkSocket cksocket = new CkSocket();
			System.out.println(cksocket.lastErrorText());
			// Connect to an SSH server:
			String hostname = rs.getString("sever_Address"); // drive.ecepvn.org
			int port = rs.getInt("port_Sever");// 2227
			boolean success = ssh.Connect(hostname, port);
			if (success != true) {
				System.out.println(ssh.lastErrorText());
				sendMailWithTSL(mail_Recieved, "Connection failed because sever_Address or port_Sever wrong");
				return false;
			}
			// Wait a max of 5 seconds when reading responses..
			ssh.put_IdleTimeoutMs(5000);
			String usernamesever = rs.getString("username_Sever");// "guest_access";
			String passSever = rs.getString("password_Sever");// "123456";
			// Authenticate using login/password:
			success = ssh.AuthenticatePw(usernamesever, passSever);
			if (success != true) {
				System.out.println(ssh.lastErrorText());
				sendMailWithTSL(mail_Recieved, "Connection failed because username_Sever or password_Sever wrong");
				return false;
			}
			CkScp scp = new CkScp();
			success = scp.UseSsh(ssh);
			if (success != true) {
				sendMailWithTSL(mail_Recieved, "Connection failed because ssh have problem");
				System.out.println(scp.lastErrorText());
				return false;
			}
			scp.put_HeartbeatMs(200);
			String filenameconf = rs.getString("file_Name_Config");// + rs.getString(7);// "*.txt";
			String extension = rs.getString("extension");// + rs.getString(7);// "*.txt";
			scp.put_SyncMustMatch(filenameconf + "." + extension);
			String remoteDir = rs.getString("remote_DirSever");// "/volume1/ECEP/song.nguyen/DW_2020/data";
			String localDir = rs.getString("local_Dir");// "C:\\Users\\Admin\\Downloads\\datawarehouse";
			int mode = 6;
			boolean bRecurse = false;
			success = scp.SyncTreeDownload(remoteDir, localDir, mode, bRecurse);

			System.out.println(filenameconf+"-----aaa");
			if (success != true) {
				System.out.println(scp.lastErrorText());
				sendMailWithTSL(mail_Recieved, "Connection failed because file_Name_Config or extension or remote_DirSever or local_Dir wrong");
				return false;
			}
			logs();
			System.out.println("SCP download matching success.");
			// Disconnect
			ssh.Disconnect();
		}
		rs.close();
		pre.close();
		conn.close();
		return true;
	}

	public static void logs() throws ClassNotFoundException, SQLException, IOException {
		Connection conn = DBConnection.getConnectionControl();
		String sql = "select * from myconfig";
		PreparedStatement pre = conn.prepareStatement(sql);
		ResultSet rs = null;
		rs = pre.executeQuery();
		rs.beforeFirst();
		while (rs.next()) {
			String dirlocal = rs.getString("local_Dir");
			File folder = new File(dirlocal);
			if (!folder.exists()) {
				System.out.println("folder not exists!");
			} else {
				File[] listfile = folder.listFiles();
				int idconf = rs.getInt("id");
				String timedownload = "now()";
				String status = "ER";
//				String extension = rs.getString("extension");

				for (File file : listfile) {
					String filename = file.getName();
					// check exist
//					WatchService watcher = FileSystems.getDefault().newWatchService();
//					Path path = FileSystems.getDefault().getPath(dirlocal);

					// insert logs
					String sqlInsert = "insert into log values(null," + idconf + "," + "'" + filename + "'" + ","
							+ timedownload + "," + "'" + dirlocal + "'" + "," + "'" + status + "'" + "," + "null"
							+ ",null,null" + ")";
					System.out.println(sqlInsert);
					PreparedStatement preinsert = conn.prepareStatement(sqlInsert);
					
					
					
					
					
					preinsert.execute();
				}
			}
		}
	}

	public static boolean sendMailWithTSL(String mail_received, String bodyMail) {
		String mail = "tn6876977@gmail.com";
		String pass = "Datawarehouse123";
		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");
	        
		Session session = Session.getInstance(props, new Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(mail, pass);
			}
		});
		try {
			MimeMessage message = new MimeMessage(session);
			message.setHeader("Content-Type", "text/plain; charset=UTF-8");
			message.setFrom(new InternetAddress(mail));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(mail_received));
			message.setSubject("Datawahouse_tuesday_456", "UTF-8");
			message.setText(bodyMail, "UTF-8");
			Transport.send(message);
			System.out.println("Message sent successfully");
		} catch (MessagingException e) {
			System.out.println(e);
			return false;
		}
		return true;
	}
//	@SuppressWarnings({ "unused", "null" })
//	private void register(Path dir) throws IOException {
//		final Kind<?> ENTRY_CREATE = null;
//		final WatchService watcher = FileSystems.getDefault().newWatchService();
//		final Map<WatchKey, Path> keys = null;
//		final boolean recursive ;
//		boolean trace = false;
//        WatchKey key = dir.register(watcher, ENTRY_CREATE);
//        if (trace) {
//            Path prev = keys.get(key);
//            if (prev == null) {
//                System.out.format("register: %s\n", dir);
//            } else {
//                if (!dir.equals(prev)) {
//                    System.out.format("update: %s -> %s\n", prev, dir);
//                }
//            }
//        }
//        keys.put(key, dir);
//    }

	public static void main(String argv[]) throws ClassNotFoundException, SQLException, IOException {
		downloadFile();
//		logs();
//		sendMailWithTSL("manhmeo1398@gmail.com", "test mail");
	}
}