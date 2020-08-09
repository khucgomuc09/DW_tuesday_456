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

	public static boolean downloadFile(String id) throws SQLException, ClassNotFoundException, IOException {
		//connect DB  (1)
		Connection conn = DBConnection.getConnectionControl();
		String sql = "select * from myconfig where id=" + id;
		PreparedStatement pre = conn.prepareStatement(sql);
		ResultSet rs = pre.executeQuery();
		rs.beforeFirst();
		while (rs.next()) {
			String mail_Recieved = rs.getString("mail_Recieved");
			//load library (2)
			try {
				System.loadLibrary("chilkat");
			} catch (UnsatisfiedLinkError e) {
				System.err.println("library failed to load.\n" + e);
				sendMailWithTSL(mail_Recieved, "library failed to load"); //(3) 
				System.exit(1);
			}

			// unlock ckglobal 
			CkGlobal glob = new CkGlobal();
			boolean globsuccess = glob.UnlockBundle("unlocked"); //(5)
			if (globsuccess != true) {
				sendMailWithTSL(mail_Recieved, "Connection failed because ckGlobal locked"); //(6)
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
			// Connect to server:
			String hostname = rs.getString("sever_Address"); // drive.ecepvn.org //(4)
			int port = rs.getInt("port_Sever");// 2227 //(4)
			boolean success = ssh.Connect(hostname, port); //(5)
			if (success != true) {
				System.out.println(ssh.lastErrorText());
				sendMailWithTSL(mail_Recieved, "Connection failed because sever_Address or port_Sever wrong"); //(6)
				return false;
			}
			// Wait a max of 5 seconds when reading responses..
			ssh.put_IdleTimeoutMs(5000);
			String usernamesever = rs.getString("username_Sever");// "guest_access"; //(4)
			String passSever = rs.getString("password_Sever");// "123456"; //(4)
			// Authenticate using login/password:
			success = ssh.AuthenticatePw(usernamesever, passSever); //(5)
			if (success != true) {
				System.out.println(ssh.lastErrorText()); 
				sendMailWithTSL(mail_Recieved, "Connection failed because username_Sever or password_Sever wrong"); //(6)
				return false;
			}
			CkScp scp = new CkScp();
			scp.put_HeartbeatMs(200);

			success = scp.UseSsh(ssh); //(5)
			if (success != true) {
				sendMailWithTSL(mail_Recieved, "Connection failed because ssh have problem"); //(6)
				System.out.println(scp.lastErrorText());
				return false;
			}
			int mode = 6;
			// Do not recurse the remote directory tree.  Only download files matching *.pem
		    // from the given remote directory.
			boolean bRecurse = false; //(5)

			String filenameconf = rs.getString("file_Name_Config");// + rs.getString(7);// "*.txt"; //(4)
			String extension = rs.getString("extension");// + rs.getString(7);// "*.txt"; //(4)
			scp.put_SyncMustMatch(filenameconf + "." + extension); //(4)
			String remoteDir = rs.getString("remote_DirSever");// "/volume1/ECEP/song.nguyen/DW_2020/data"; //(4)
			String localDir = rs.getString("local_Dir");// "C:\\Users\\Admin\\Downloads\\datawarehouse"; //(4)

			success = scp.SyncTreeDownload(remoteDir, localDir, mode, bRecurse);//(5)

			if (success != true) {
				System.out.println(scp.lastErrorText());
				sendMailWithTSL(mail_Recieved, //(5)
						"Connection failed because file_Name_Config or extension or remote_DirSever or local_Dir wrong");
				return false;
			}
			System.out.println("SCP download matching success.");
			logs(id); //(7)
			ssh.Disconnect();

		}
		rs.close();
		pre.close();
		conn.close();
		return true;
	}

	public static void logs(String id) throws ClassNotFoundException, SQLException, IOException {
	// connect DB
		Connection conn = DBConnection.getConnectionControl();
		String sql = "select local_Dir from myconfig"; //(7)
		String timedownload = "now()"; //(7)
		String status = "ER"; //(7)
		String selectfilename = "select 1 from log where file_name=?"; //(7)
		PreparedStatement pre = conn.prepareStatement(sql);
		ResultSet rs = null;
		rs = pre.executeQuery();
		rs.beforeFirst();
		while (rs.next()) {
			String dirlocal = rs.getString("local_Dir");
			File folder = new File(dirlocal);

			if (!folder.exists()) { // check folder exists 
				System.out.println("folder not exists!");
			} else {
				File[] listfile = folder.listFiles();
				for (File file : listfile) { // select each file in list file //(7)
					boolean isExist = false;
//					System.out.println(isExist);
					String filename = file.getName(); //get name file in folder //(7)
					// check exist
					PreparedStatement prep = conn.prepareStatement(selectfilename);
					prep.setString(1, filename);// get name file in log //(7)
					ResultSet rss = prep.executeQuery();
					if (rss.next()) {
						isExist = true;
//						continue;
					}

//					System.out.println("after: " + isExist);
					if (isExist == false) {// insert log if file not exist in log //(8)
						// insert logs
						String sqlInsert = "insert into log values(null," + id + "," + "'" + filename + "',null,"
								+ timedownload + "," + "'" + dirlocal + "'" + "," + "'" + status + "'" + "," + "null"
								+ ",null" + ")";
						PreparedStatement preinsert = conn.prepareStatement(sqlInsert);
						preinsert.execute();
						System.out.println(sqlInsert);
					} else {
						System.out.println(filename + " existed"); //(9)
					}
				}
			}
		}
	}

	public static boolean sendMailWithTSL(String mail_received, String bodyMail) {
		final String mail = "tn6876977@gmail.com";
		final String pass = "Datawarehouse123";
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

	public static void main(String[] args) {
		try {
//			logs(1+"");
			downloadFile("" + 1);
		} catch (ClassNotFoundException | SQLException | IOException e) {
			e.printStackTrace();
		}
	}
}