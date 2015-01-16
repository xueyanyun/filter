package realtime.filter.filter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;



public class RealTimeFilterBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	
	private HTable hTable;
	private Jedis jedis;//是不是同一个实例
	private int recNumber = 10;//type1,type2,type3的数量是一样的吗？
	
	private static final Logger log = Logger.getLogger(RealTimeFilterBolt.class);
	
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		//hbase,真正版本注意配置
		Configuration conf = HBaseConfiguration.create();
    	conf.set("hbase.zookeeper.quorum", "eb179,eb178,eb177");
    	conf.set("hbase.zookeeper.property.clientPort","2181");    	
    	
    	try {
			hTable = new HTable(conf, "t2");
		} catch (IOException e) {
			e.printStackTrace();
		}   	
    	System.out.println("bolt SaveResultBolt init Hbase ok! ");
    	System.out.println(hTable == null);
		
		//redi    	
		jedis = new Jedis("10.1.70.4", 3030);	

	}


	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		long start = System.currentTimeMillis();
		log.info(Thread.currentThread().getName() + "'s execute() begin:" + start);

		
		String userId = input.getStringByField("user");
		String bookId = input.getStringByField("book");		
		String type = input.getStringByField("type");	
		
		//获取用户阅读历史
		List<String> hisBooks = new ArrayList<String>();
		Get get = new Get(Bytes.toBytes(userId));
		Result result = null;
		try {
			result = hTable.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (KeyValue item: result.list()) {
			hisBooks.add(Bytes.toString(item.getQualifier()));
		}		

		JSONObject object = new JSONObject();
		if ("3".equals(type)) {
			object.put("read", compute(hisBooks, bookId, "1"));
			object.put("order", compute(hisBooks, bookId, "2"));
		}else {		
			if ("1".equals(type)) {
				object.put("read", compute(hisBooks, bookId, type));
			}else {
				object.put("order", compute(hisBooks, bookId, type));
			}
		}		
		
		log.info("json result: " + object.toString());
		//######真正运行的时候 写过期时间################
		//jedis.setex(key, seconds, value)
		jedis.set("corre_rec_realtime_filter_result:" + userId + ":" + bookId + ":" + type, object.toString());
		

		log.info(Thread.currentThread().getName() + "'s execute() end:" + System.currentTimeMillis());
		log.info(Thread.currentThread().getName() + "'s execute() cost:" + (System.currentTimeMillis() - start));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		
	}
	
	private Map<String, String> compute(List<String> historyBooks, String bookId, String type) {
	
		List<String>  offLineResult = null;
		//获取离线推荐结果
		if ("1".equals(type)) {//阅读还阅读
			offLineResult = jedis.lrange("corre_rec_read_offline_result:"  + bookId, 0, -1);
		} else if ("2".equals(type)) {//订购还订购
			offLineResult = jedis.lrange("corre_rec_order_offline_result:"  + bookId, 0, -1);
		}
		
		Map<String, String> selectedBookMap = new LinkedHashMap<String, String>();
		if (offLineResult.isEmpty()) {
			return selectedBookMap;
		}	
		
		Map<String, String> backupBookMap = new LinkedHashMap<String, String>();
		for (Iterator<String> iter = offLineResult.iterator(); iter.hasNext();) {
			if (selectedBookMap.size() >= recNumber) {
				break;
			}
			String recBookId = iter.next();
			String confidence = iter.next();
			if (!historyBooks.contains(recBookId)) {
				selectedBookMap.put(recBookId, confidence);
			} else {
				backupBookMap.put(recBookId, confidence);
			}				
		}
		
		for (Entry<String, String> entry : backupBookMap.entrySet()) {
			if (selectedBookMap.size() >= recNumber) {
				break;				
			}		
			selectedBookMap.put(entry.getKey(), entry.getValue());			
		} 
		return selectedBookMap;	
	}
	
}

///*关于可靠性的问题需要确认*/
//public class RealTimeFilterBolt extends BaseRichBolt {
//
//	private static final long serialVersionUID = 1L;
//	
//	private HTable hTable;
//	private Jedis jedis;//是不是同一个实例
//	private int recNumber = 10;//type1,type2,type3的数量是一样的吗？
//	
//	private static final Logger log = Logger.getLogger(RealTimeFilterBolt.class);
//	private OutputCollector collector;
//	
//	
//	@Override
//	public void prepare(Map stormConf, TopologyContext context,
//			OutputCollector collector) {
//		//hbase,真正版本注意配置
//		Configuration conf = HBaseConfiguration.create();
//    	conf.set("hbase.zookeeper.quorum", "eb179,eb178,eb177");
//    	conf.set("hbase.zookeeper.property.clientPort","2181");    	
//    	
//    	try {
//			hTable = new HTable(conf, "t2");
//		} catch (IOException e) {
//			e.printStackTrace();
//		}   	
//    	System.out.println("bolt SaveResultBolt init Hbase ok! ");
//    	System.out.println(hTable == null);
//		
//		//redi    	
//		jedis = new Jedis("10.1.70.4", 3030);	
//		
//		this.collector = collector;
//	}
//	
//	
//	@Override
//	public void execute(Tuple input) {
//		//test
//		long start = System.currentTimeMillis();
//		log.info(Thread.currentThread().getName() + "'s execute() begin:" + start);
//
//		
//		String userId = input.getStringByField("user");
//		String bookId = input.getStringByField("book");		
//		String type = input.getStringByField("type");	
//		
//		//获取用户阅读历史
//		List<String> hisBooks = new ArrayList<String>();
//		Get get = new Get(Bytes.toBytes(userId));
//		Result result = null;
//		try {
//			result = hTable.get(get);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		for (KeyValue item: result.list()) {
//			hisBooks.add(Bytes.toString(item.getQualifier()));
//		}		
//
//		JSONObject object = new JSONObject();
//		if ("3".equals(type)) {
//			object.put("read", compute(hisBooks, bookId, "1"));
//			object.put("order", compute(hisBooks, bookId, "2"));
//		}else {		
//			if ("1".equals(type)) {
//				object.put("read", compute(hisBooks, bookId, type));
//			}else {
//				object.put("order", compute(hisBooks, bookId, type));
//			}
//		}
//		
//		//test
//		log.info("json result: " + object.toString());
//		//真正运行的时候 写过期时间
//		jedis.set("corre_rec_realtime_filter_result:" + userId + ":" + bookId + ":" + type, object.toString());
//		//jedis.setex(key, seconds, value)
//		//test
//
//		
//		
//		//test
//		collector.ack(input);
//		log.info(Thread.currentThread().getName() + "'s ack end:" + System.currentTimeMillis());
//		log.info(Thread.currentThread().getName() + "'s execute() end:" + System.currentTimeMillis());
//		log.info(Thread.currentThread().getName() + "'s execute() cost:" + (System.currentTimeMillis() - start));
//		//test
//		
//	}
//	
//	private Map<String, String> compute(List<String> historyBooks, String bookId, String type) {
//
//		List<String>  offLineResult = null;
//		//获取离线推荐结果
//		if ("1".equals(type)) {//阅读还阅读
//			offLineResult = jedis.lrange("corre_rec_read_offline_result:"  + bookId, 0, -1);
//		} else if ("2".equals(type)) {//订购还订购
//			offLineResult = jedis.lrange("corre_rec_order_offline_result:"  + bookId, 0, -1);
//		}
//		
//		Map<String, String> selectedBookMap = new LinkedHashMap<String, String>();
//		if (offLineResult.isEmpty()) {
//			return selectedBookMap;
//		}	
//		
//		Map<String, String> backupBookMap = new LinkedHashMap<String, String>();
//		for (Iterator<String> iter = offLineResult.iterator(); iter.hasNext();) {
//			if (selectedBookMap.size() >= recNumber) {
//				break;
//			}
//			String recBookId = iter.next();
//			String confidence = iter.next();
//			if (!historyBooks.contains(recBookId)) {
//				selectedBookMap.put(recBookId, confidence);
//			} else {
//				backupBookMap.put(recBookId, confidence);
//			}				
//		}
//		
//		for (Entry<String, String> entry : backupBookMap.entrySet()) {
//			if (selectedBookMap.size() >= recNumber) {
//				break;				
//			}		
//			selectedBookMap.put(entry.getKey(), entry.getValue());			
//		} 
//		return selectedBookMap;	
//	}
//	
//	
//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		
//	}
//
//}
