import java.io.*;
import java.sql.*;
import java.util.*;
import org.json.*;

public class PopData {
	private String[] cates = {"Active Life", "Arts & Entertainment", "Automotive", "Car Rental", "Cafes", "Beauty & Spas", "Convenience Stores", "Dentists",
			"Doctors", "Drugstores", "Department Stores", "Education", "Event Planning & Services", "Flowers & Gifts", "Food", "Health & Medical", 
			"Home Services", "Home & Garden", "Hospitals", "Hotels & Travel", "Hardware Stores", "Grocery", "Medical Centers", "Nurseries & Gardening", 
			"Nightlife", "Restaurants", "Shopping", "Transportation"};
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
		String sql = "INSERT INTO MAIN_CATE VALUES (?, ?)";
		
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
	
	public void popUserFriend() {
		String sql = "INSERT INTO USER_FRIENDS VALUES (?, ?)";
		
		BufferedReader br = null;
		FileReader fr = null;
		
		try {
			System.out.println("Start inserting user friends data...");
			
			stat = conn.prepareStatement(sql);
			
			fr = new FileReader(user);
			br = new BufferedReader(fr);
			
			int count = 0;
			while(br.ready()) {
				
				JSONObject json = new JSONObject(br.readLine());
				JSONArray jay = json.getJSONArray("friends");
				
				for(Object friend: jay) {
					stat.setString(1, json.getString("user_id"));
					stat.setString(2, friend.toString());
					stat.executeUpdate();
				}
				
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
	
	public void popUser() {
		String sql = "INSERT INTO USERS VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		
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
				stat.setInt(8, json.getJSONObject("votes").getInt("useful"));
				stat.setInt(9, json.getJSONObject("votes").getInt("funny"));
				stat.setInt(10, json.getJSONObject("votes").getInt("cool"));
				
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
	
	public void popSubCat() {
		String sql = "INSERT INTO SUB_CATE VALUES (?, ?)";
		
		BufferedReader br = null;
		FileReader fr = null;
		
		try {
			System.out.println("Start inserting sub_cate data...");
			
			stat = conn.prepareStatement(sql);
			
			fr = new FileReader(business);
			br = new BufferedReader(fr);
			
			int count = 0;
			while(br.ready()) {
				
				JSONObject json = new JSONObject(br.readLine());
				
				JSONArray ar = json.getJSONArray("categories");
				
				stat.setString(1, json.getString("business_id"));
				
				for(int i=0; i<ar.length(); i++) {		
					String subName = ar.getString(i);
					int index = Arrays.binarySearch(cates, subName);
					
					if(index < 0) {
						stat.setString(2, subName);
						stat.executeUpdate();

						stat.setString(1, json.getString("business_id"));
					}
				}
				
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
	
	//business
	public void popBusiness() {
		String sql = "INSERT INTO BUSINESS VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		
		BufferedReader br = null;
		FileReader fr = null;
		
		try {
			System.out.println("Start inserting business data...");
			
			stat = conn.prepareStatement(sql);
			
			fr = new FileReader(business);
			br = new BufferedReader(fr);
			
			int count = 0;
			while(br.ready()) {
				
				JSONObject json = new JSONObject(br.readLine());
				
				stat.setString(1, json.getString("business_id"));
				stat.setString(2, json.getString("full_address"));
				stat.setInt(3, json.getBoolean("open")? 1: 0);
				
				JSONArray ar = json.getJSONArray("categories");
				for(int i=0; i<ar.length(); i++) {				
					int index = Arrays.binarySearch(cates, ar.getString(i));
					
					if(index >= 0) {
						stat.setInt(4, index + 1);
						break;
					}
				}
				
				stat.setString(5, json.getString("city"));
				stat.setString(6, json.getString("state"));
				stat.setDouble(7, json.getDouble("latitude"));
				stat.setDouble(8, json.getDouble("longitude"));
				stat.setInt(9, json.getInt("review_count"));
				stat.setString(10, json.getString("name"));
				stat.setInt(11, json.getInt("stars"));
				stat.setString(12, json.getString("type"));
				
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
	
	public void popHour() {
		String sql = "INSERT INTO HOURS VALUES (?, ?, ?, ?)";
		
		BufferedReader br = null;
		FileReader fr = null;
		
		try {
			System.out.println("Start inserting hours data...");
			
			stat = conn.prepareStatement(sql);
			
			fr = new FileReader(business);
			br = new BufferedReader(fr);
			
			int count = 0;
			while(br.ready()) {
				
				JSONObject json = new JSONObject(br.readLine());
				
				stat.setString(1, json.getString("business_id"));
				Set<String> k = json.getJSONObject("hours").keySet();
				
				for(String week: k) {
					JSONObject weekObj = json.getJSONObject("hours").getJSONObject(week);
					
					stat.setString(2, week);// week
					stat.setString(3, weekObj.getString("open"));
					stat.setString(4, weekObj.getString("close"));
					
					stat.executeUpdate();
					
					stat.setString(1, json.getString("business_id"));
				}
				
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
	
	/*public void popUserVote() {
		String sql = "INSERT INTO USER_VOTES VALUES (?, ?, ?)";
		
		BufferedReader br = null;
		FileReader fr = null;
		
		try {
			System.out.println("Start inserting user vote data...");
			
			stat = conn.prepareStatement(sql);
			
			fr = new FileReader(user);
			br = new BufferedReader(fr);
			
			int count = 0;
			while(br.ready()) {
				
				JSONObject json = new JSONObject(br.readLine());
				Set<String> k = json.getJSONObject("votes").keySet();
				
				for(String type: k) {
					stat.setString(1, json.getString("review_id"));
					stat.setString(2, type);
					stat.setInt(3, json.getJSONObject("votes").getInt(type));
					stat.executeUpdate();
				}
				
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
	}*/
	
	/*public void popRevVote() {
		String sql = "INSERT INTO REV_VOTES VALUES (?, ?, ?)";
		
		BufferedReader br = null;
		FileReader fr = null;
		
		try {
			System.out.println("Start inserting review vote data...");
			
			stat = conn.prepareStatement(sql);
			
			fr = new FileReader(review);
			br = new BufferedReader(fr);
			
			int count = 0;
			while(br.ready()) {
				
				JSONObject json = new JSONObject(br.readLine());
				Set<String> k = json.getJSONObject("votes").keySet();
				
				for(String type: k) {
					stat.setString(1, json.getString("review_id"));
					stat.setString(2, type);
					stat.setInt(3, json.getJSONObject("votes").getInt(type));
					stat.executeUpdate();
				}
				
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
	}*/
	
	public void popCheckin() {
		String sql = "INSERT INTO CHECKIN VALUES (?, ?, ?, ?)";
		
		BufferedReader br = null;
		FileReader fr = null;
		
		try {
			System.out.println("Start inserting checkin data...");
			
			stat = conn.prepareStatement(sql);
			
			fr = new FileReader(checkin);
			br = new BufferedReader(fr);
			
			int count = 0;
			while(br.ready()) {
				
				JSONObject json = new JSONObject(br.readLine());
				
				stat.setString(1, json.getString("business_id"));
				
				JSONObject obj = json.getJSONObject("checkin_info");
				Set<String> set = obj.keySet();
				
				for(String info: set) {
					stat.setInt(2, Integer.parseInt(info.split("-")[1]));//day
					stat.setInt(3, Integer.parseInt(info.split("-")[0]));//hour
					stat.setInt(4, json.getJSONObject("checkin_info").getInt(info));//count

					stat.executeUpdate();
					
					stat.setString(1, json.getString("business_id"));
					
				}
				
				//System.out.println(count);
				
				if(count % 1000 == 0)
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
	
	public void popReview() {
		String sql = "INSERT INTO REVIEWS VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
		
		BufferedReader br = null;
		FileReader fr = null;
		
		try {
			System.out.println("Start inserting reviews data...");
			
			stat = conn.prepareStatement(sql);
			
			fr = new FileReader(review);
			br = new BufferedReader(fr);
			
			int count = 0;
			while(br.ready()) {
				
				JSONObject json = new JSONObject(br.readLine());
				
				stat.setString(1, json.getString("review_id"));
				stat.setString(2, json.getString("user_id"));
				stat.setInt(3, json.getInt("stars"));
				stat.setDate(4, java.sql.Date.valueOf(json.getString("date")));
				//stat.setString(5, json.getString("text"));
				stat.setString(5, json.getString("type"));
				stat.setString(6, json.getString("business_id"));
				stat.setInt(7, json.getJSONObject("votes").getInt("useful"));
				stat.setInt(8, json.getJSONObject("votes").getInt("funny"));
				stat.setInt(9, json.getJSONObject("votes").getInt("cool"));
				
				
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
	
	public void closeDB() {
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
