import java.sql.*;

public class DeleteData {
	private String host = "localhost";
	private String port = "1521";
	private String dbName = "XE";
	private String userName = "Howard4";
	private String password = "Howard12345";
	private String oracleURL = "jdbc:oracle:thin:@" + host + ":" + port + ":" + dbName;
	private Connection conn = null;
	private Statement stat = null;
	
	public DeleteData() {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection(oracleURL, userName, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void deleteAll() {
		String sqlUser = "DELETE FROM USERS";
		String sqlMainCat = "DELETE FROM MAIN_CATE";
		String sqlBus = "DELETE FROM BUSINESS";
		String sqlCheck = "TRUNCATE TABLE CHECKIN";//"DELETE FROM CHECKIN";
		String sqlHour = "TRUNCATE TABLE HOURS";//"DELETE FROM HOURS";
		String sqlRev = "TRUNCATE TABLE REVIEWS";//"DELETE FROM REVIEWS";
		String sqlUserFriend = "TRUNCATE TABLE USER_FRIENDS";//"DELETE FROM USER_FRIENDS";
		String sqlSubCat = "TRUNCATE TABLE SUB_CATE";//"DELETE FROM SUB_CATE";
		
		try {
			System.out.println("Start deleting data...");
			
			conn.setAutoCommit(false);
			stat = conn.createStatement();
			
			stat.executeUpdate(sqlCheck);
			stat.executeUpdate(sqlUserFriend);
			//stat.executeUpdate(sqlUserVote);
			//stat.executeUpdate(sqlRevVote);
			stat.executeUpdate(sqlRev);
			stat.executeUpdate(sqlUser);
			stat.executeUpdate(sqlHour);
			stat.executeUpdate(sqlSubCat);
			stat.executeUpdate(sqlBus);
			stat.executeUpdate(sqlMainCat);
			
			conn.commit();
		} catch (Exception e1) {
			try {
				conn.rollback();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			e1.printStackTrace();
		}
		
		System.out.println("Done");
	}
	
	public void closeDB() {
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
