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
		String sqlCheck = "DELETE FROM CHECKIN";
		String sqlHour = "DELETE FROM HOURS";
		String sqlRevVote = "DELETE FROM REV_VOTES";
		String sqlRev = "DELETE FROM REVIEWS";
		String sqlUserFriend = "DELETE FROM USER_FRIENDS";
		String sqlUserVote = "DELETE FROM USER_VOTES";
		
		try {
			System.out.println("Start deleting data...");
			
			conn.setAutoCommit(false);
			stat = conn.createStatement();
			
			stat.executeUpdate(sqlUser);
			stat.executeUpdate(sqlMainCat);
			stat.executeUpdate(sqlBus);
			stat.executeUpdate(sqlCheck);
			stat.executeUpdate(sqlHour);
			stat.executeUpdate(sqlRevVote);
			stat.executeUpdate(sqlRev);
			stat.executeUpdate(sqlUserFriend);
			stat.executeUpdate(sqlUserVote);
			
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
