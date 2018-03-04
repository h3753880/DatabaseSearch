import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Set;

import org.json.JSONObject;

public class Populate {

	public static void main(String[] args) {
		//delete data
		DeleteData deleteData = new DeleteData();
		
		deleteData.deleteAll();
		
		deleteData.closeDB();
		
		//test();
		
		//insert data
		PopData popData = new PopData(args);
		
		popData.popMainCate();
		popData.popBusiness();
		popData.popSubCat();
		popData.popHour();
		popData.popUser();
		popData.popReview();
		popData.popUserFriend();
		
		popData.closeDB();
		
	}
	
	public static void test() {
		BufferedReader br = null;
		FileReader fr = null;
		
		try {
			
			fr = new FileReader("yelp_user.json");
			br = new BufferedReader(fr);
			
			int count = 0;
			while(br.ready()) {
				
				JSONObject json = new JSONObject(br.readLine());
				
				
				if(count == 5) {
					Set<String> k = json.getJSONObject("friends").keySet();
					
					for(String friend: k) {
						System.out.println(json.getJSONObject("friends").getString(friend));
					}
					System.out.println(json.get("friends"));
					break;
				}
				count++;
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
