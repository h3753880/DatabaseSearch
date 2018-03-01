import java.io.BufferedReader;
import java.io.FileReader;
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
		popData.popUser();
		
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
				
				
				if(count == 65) {
					System.out.println(json.get("elite"));
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
