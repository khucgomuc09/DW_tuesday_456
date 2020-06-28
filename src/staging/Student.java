package staging;

public class Student {
	private String stt ;
	private String mssv;
	private String firstname;
	private String lastname;
	private String date;
	private String malop;
	private String tenlop;
	private String sdt;
	private String email;
	private String quequan;
	private String note;
	
	public Student() {
	}

	
	public Student(String stt, String mssv, String firstname, String lastname, String date, String malop, String tenlop,
			String sdt, String email, String quequan, String note) {
		super();
		this.stt = stt;
		this.mssv = mssv;
		this.firstname = firstname;
		this.lastname = lastname;
		this.date = date;
		this.malop = malop;
		this.tenlop = tenlop;
		this.sdt = sdt;
		this.email = email;
		this.quequan = quequan;
		this.note = note;
	}


	public String getStt() {
		return stt;
	}

	public void setStt(String stt) {
		this.stt = stt;
	}
	

	public String getMssv() {
		return mssv;
	}


	public void setMssv(String mssv) {
		this.mssv = mssv;
	}


	public String getFirstname() {
		return firstname;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getMalop() {
		return malop;
	}

	public void setMalop(String malop) {
		this.malop = malop;
	}

	public String getTenlop() {
		return tenlop;
	}

	public void setTenlop(String tenlop) {
		this.tenlop = tenlop;
	}

	public String getSdt() {
		return sdt;
	}

	public void setSdt(String sdt) {
		this.sdt = sdt;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getQuequan() {
		return quequan;
	}

	public void setQuequan(String quequan) {
		this.quequan = quequan;
	}

	public String getNote() {
		return note;
	}

	public void setNote(String note) {
		this.note = note;
	}


	@Override
	public String toString() {
		return stt +", "+ mssv   +", "+ firstname +", " + lastname +", " + date   +", "+ malop   +", "+ tenlop +", " + sdt  +", "+ email +", "+ quequan +", " + note+"\n";
	}
	
	
	

}
