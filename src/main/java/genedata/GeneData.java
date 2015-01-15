package genedata;

import redis.clients.jedis.Jedis;


public class GeneData {
	public static void main(String[] args) {
		Jedis jedis = new Jedis("10.1.70.4", 3030);
//		String readResultTable = "corre_rec_read_offline_result";
//		String srcBookId = "book1";
//		String key = readResultTable+ ":" + srcBookId;
//		double value = 0.5;
//		for (int i = 90; i < 120; i++) {
//			//jedis.hset(key, "book"+i, String.valueOf(value));	
//			jedis.rpush(key, "book"+i);
//			jedis.rpush(key, String.valueOf(value));			
//			value -= 0.01;
//		}
//		
		
		
		String readResultTable = "corre_rec_order_offline_result";
		String srcBookId = "book1";
		String key = readResultTable+ ":" + srcBookId;
		double value = 0.5;
		for (int i = 90; i < 105; i++) {
			jedis.rpush(key, "book"+i);
			jedis.rpush(key, String.valueOf(value));			
			value -= 0.01;
		}
		
	}
	
}
