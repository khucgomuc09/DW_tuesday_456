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
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

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

	public static boolean downloadFile(String id) throws SQLException, ClassNotFoundException, IOException {
		Connection conn = DBConnection.getConnectionControl();
		String sql = "select * from myconfig where id=" + id;
		PreparedStatement pre = conn.prepareStatement(sql);
		ResultSet rs = pre.executeQuery();
		rs.beforeFirst();
		while (rs.next()) {
			String mail_Recieved = rs.getString("mail_Recieved");

			// unlock ckglobal
			CkGlobal glob = new CkGlobal();
			boolean globsuccess = glob.UnlockBundle("unlocked");
			if (globsuccess != true) {
				sendMailWithTSL(mail_Recieved, "Connection failed because ckGlobal locked");
				System.out.println(glob.lastErrorText());
				return false;
			}
			int status = glob.get_UnlockStatus();
			if (status == 2) {
				System.out.println("Unlocked using purchased unlock code.");
			} else {
				System.out.println("Unlocked in trial mode.");
			}
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
			scp.put_HeartbeatMs(200);

			success = scp.UseSsh(ssh);
			if (success != true) {
				sendMailWithTSL(mail_Recieved, "Connection failed because ssh have problem");
				System.out.println(scp.lastErrorText());
				return false;
			}
			int mode = 6;
			boolean bRecurse = false;

			String filenameconf = rs.getString("file_Name_Config");// + rs.getString(7);// "*.txt";
			String extension = rs.getString("extension");// + rs.getString(7);// "*.txt";
			scp.put_SyncMustMatch(filenameconf + "." + extension);
			String remoteDir = rs.getString("remote_DirSever");// "/volume1/ECEP/song.nguyen/DW_2020/data";
			String localDir = rs.getString("local_Dir");// "C:\\Users\\Admin\\Downloads\\datawarehouse";

			success = scp.SyncTreeDownload(remoteDir, localDir, mode, bRecurse);

			if (success != true) {
				System.out.println(scp.lastErrorText());
				sendMailWithTSL(mail_Recieved,
						"Connection failed because file_Name_Config or extension or remote_DirSever or local_Dir wrong");
				return false;
			}
			System.out.println("SCP download matching success.");
			logs(id);
			ssh.Disconnect();

		}
		rs.close();
		pre.close();
		conn.close();
		return true;
	}

	public static void logs(String id) throws ClassNotFoundException, SQLException, IOException {
		Connection conn = DBConnection.getConnectionControl();
		String sql = "select local_Dir from myconfig";
		String timedownload = "now()";
		String status = "ER";
		String selectfilename = "select 1 from log where file_name=?";
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
				for (File file : listfile) {
					boolean isExist = false;
					System.out.println(isExist);
					String filename = file.getName();
					// check exist
					PreparedStatement prep = conn.prepareStatement(selectfilename);
					prep.setString(1, filename);
					ResultSet rss = prep.executeQuery();
					if (rss.next()) {
						isExist = true;
					}

					System.out.println("after: " + isExist);
					if (isExist == false) {
						// insert logs
						String sqlInsert = "insert into log values(null," + id + "," + "'" + filename + "',null,"
								+ timedownload + "," + "'" + dirlocal + "'" + "," + "'" + status + "'" + "," + "null"
								+ ",null" + ")";
						PreparedStatement preinsert = conn.prepareStatement(sqlInsert);
						preinsert.execute();
						System.out.println(sqlInsert);
					} else {
						System.out.println(filename + " existed");
						continue;
					}
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

	public static void main(String argv[]) throws ClassNotFoundException, SQLException, IOException {
<<<<<<< HEAD
//		downloadFile("3");
		logs("1");
=======
		downloadFile("3");
>>>>>>> 889284d54d7b38eb12680c810228223ce107c7e6
	}
}