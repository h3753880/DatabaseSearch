import java.io.*;
import java.sql.*;
import java.util.*;
import org.json.*;

public class PopData {
	private String user = "yelp_user.json";
	private String checkin = "yelp_checkin.json";
	private String review = "yelp_review.json";
	private String business = "yelp_business.json";
	private String host = "localhost";
	private String port = "1521";
	private String dbName = "XE";
	private String userName = "Howard4";
	private String password = "Howard12345";
	private String oracleURL = "jdbc:oracle:thin:@" + host + ":" + port + ":" + dbName;
	private Connection conn = null;
	private PreparedStatement stat = null;
	
	public PopData(String[] args) {
		for(String str: args) {
			if(str.contains("user"))
				user = str;
			else if(str.contains("review"))
				review = str;
			else if(str.contains("checkin"))
				checkin = str;
			else if(str.contains("business"))
				business = str;
		}
		
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection(oracleURL, userName, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void popMainCate() {
		String[] cates = {"Active Life", "Arts & Entertainment", "Automotive", "Car Rental", "Cafes", "Beauty & Spas", "Convenience Stores", "Dentists",
				"Doctors", "Drugstores", "Department Stores", "Education", "Event Planning & Services", "Flowers & Gifts", "Food", "Health & Medical", 
				"Home Services", "Home & Garden", "Hospitals", "Hotels & Travel", "Hardware Stores", "Grocery", "Medical Centers", "Nurseries & Gardening", 
				"Nightlife", "Restaurants", "Shopping", "Transportation"};
		
		String sql = "INSERT INTO MAIN_CATE VALUES (?, ?)";
		
		BufferedReader br = null;
		FileReader fr = null;
		
		try {
			System.out.println("Start inserting categories data...");
			
			stat = conn.prepareStatement(sql);
			
			for(int i=0; i<cates.length; i++) {
				
				stat.setInt(1, i+1);
				stat.setString(2, cates[i]);
				
				stat.executeUpdate();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			System.out.println("Done..");
		}
	}
	
	public void popUser() {
		String sql = "INSERT INTO USERS VALUES (?, ?, ?, ?, ?, ?, ?)";
		
		BufferedReader br = null;
		FileReader fr = null;
		
		try {
			System.out.println("Start inserting user data...");
			
			stat = conn.prepareStatement(sql);
			
			fr = new FileReader(user);
			br = new BufferedReader(fr);
			
			int count = 0;
			while(br.ready()) {
				
				JSONObject json = new JSONObject(br.readLine());
				
				stat.setString(1, json.getString("user_id"));
				stat.setDate(2, java.sql.Date.valueOf(json.getString("yelping_since") + "-01"));
				stat.setInt(3, json.getInt("review_count"));
				stat.setString(4, json.getString("name"));
				stat.setInt(5, json.getInt("fans"));
				stat.setDouble(6, json.getDouble("average_stars"));
				stat.setString(7, json.getString("type"));
				
				stat.executeUpdate();
				
				if(count % 10000 == 0)
					System.out.println(count);
				
				count++;
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			System.out.println("Done..");
			try {
				br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void popBusiness() {
		
	}
	
	public void popCheckin() {
		
	}
	
	public void popReview() {
		
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
