import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Set;

import org.json.JSONArray;
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
		
		popData.popCheckin();
		
		popData.closeDB();
		
	}
	
	public static void test() {
		BufferedReader br = null;
		FileReader fr = null;
		
		try {
			
			fr = new FileReader("yelp_checkin.json");
			br = new BufferedReader(fr);
			
			int count = 0;
			while(br.ready()) {
				
				JSONObject json = new JSONObject(br.readLine());
				
				
				if(count == 5) {
					
					System.out.println(json.get("checkin_info"));
					
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
