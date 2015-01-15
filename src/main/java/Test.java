import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.tools.ant.taskdefs.Sleep;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;




import redis.clients.jedis.Jedis;







public class Test {
	public static void main(String[] args) {
		
		//redis
		Jedis jedis = new Jedis("10.1.70.4", 3030);
		System.out.println(jedis);
//		String userId = "test";
//		Map<String, String> bookConfidenceMap = jedis.hgetAll("corre _rec_offline_result" + userId);
//		System.out.println(bookConfidenceMap);
//		System.out.println(bookConfidenceMap.size());
//		
//		String string = jedis.get("hello");
//		System.out.println(string);
		
		 List<String> lrange = jedis.lrange("corre_rec_read_offline_result:"  + "book2", 0, -1);
		 System.out.println(lrange.size());
		
		
		//
		
		Map<String, String>  bMap = new HashMap<String, String>();
		bMap.put("b2", "1");
		bMap.put("b5", "1");
		bMap.put("b4", "1");
		bMap.put("b3", "1");
		Iterator<Entry<String, String>> iterator = bMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, String> next = iterator.next();
			System.out.println(next.getKey());
			System.out.println(next.getValue());
			
		}
		
		
		//json
		
//		Map<String, Integer> finalResult = new HashMap<String, Integer>();//好像得用treemap
//		finalResult.put("book1", 1);
//		finalResult.put("book2", 2);
//		finalResult.put("book4", 2);
//		finalResult.put("book5", 2);
//		
//		
//		JSONArray ja1 = JSONArray.fromObject(finalResult);
//		System.out.println(ja1);
//		//
//		
//		//finalResult.putAll(null);
//		ArrayList<String> eArrayList;
		
//		Address address = new Address();
//		  address.setNo("104");
//		  address.setProvience("陕西");
//		  address.setRoad("高新路");
//		  address.setStreate("");
//		  Address address2 = new Address();
//		  address2.setNo("105");
//		  address2.setProvience("陕西");
//		  address2.setRoad("未央路");
//		  address2.setStreate("张办");
//		  List list = new ArrayList();
//		  list.add("1");
//		  list.add("2");
//		  list.add(address);
//		  list.add(address2);
//		  JSONArray json = JSONArray.fromObject(list);
//		  //log.info(json.toString());
//		  System.out.println(json.toString());
		
//		
//		 Address address = new Address();
//		    address.setNo("104");
//		    address.setProvience("陕西");
//		    address.setRoad("高新路");
//		    address.setStreate("");
//		    JSONArray json = JSONArray.fromObject(address);
//		    System.out.println(json.toString());
		
//	 Map<String, String> values = new HashMap<String, String>();
//	 values.put("book1", "value1");
//	 values.put("book2", "value2");
//	 
//	 JSONObject object = new JSONObject();
//	 object.putAll(values);
//	 
//	 System.out.println(object.toString());
//	
//	 
//	 JSONObject object2 = new JSONObject();
//	 object2.put("book1", "value1");
//	 
//	 System.out.println(object2.toString());
//	 
//	 
//	 JSONObject object1= new JSONObject();
//	 object1.put("read", object);
//	 
//	 System.out.println(object1.toString());
//	 
//	 
//	 JSONObject object4= new JSONObject();
//	 object1.put("read", object2);
//	 
//	 System.out.println(object4.toString());
//	 
//	 
//	 
//	 JSONObject object5= new JSONObject();
//	 object5.put("read", object2);
//	 object5.put("order", object);
//	 
//	 System.out.println(object5.toString());
//	 
//	 JSONArray array = JSONArray.fromObject(values);
//	 JSONObject object6= new JSONObject();
//	 object6.put("read", array);
//	 object6.put("order", object);
//	 
//	 System.out.println(object6.toString());
		int a=0;
		try {
			 a = 5 /0;
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("i am here");
		
		System.out.println("i am here");
		System.out.println("i am here");
		System.out.println("i am here");
		
	 
		a = 5 /1;
		System.out.println(a);
	 
	 
	 
		
	}

	
	

}

 