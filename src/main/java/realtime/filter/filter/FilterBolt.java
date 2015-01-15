package realtime.filter.filter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/*关于可靠性的问题需要确认*/
public class FilterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	private HTable hTable;
	private Jedis jedis;//是不是同一个实例
	private int recNumber = 10;//type1,type2,type3的数量是一样的吗？
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
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
	public void execute(Tuple input) {
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
		if ("3".equals(type)) {//注意这个时候有问题，记得改动	
			object.put("read", compute(hisBooks, bookId, "1"));
			object.put("order", compute(hisBooks, bookId, "2"));
		}else {		
			if ("1".equals(type)) {
				object.put("read", compute(hisBooks, bookId, type));
			}else {
				object.put("order", compute(hisBooks, bookId, type));
			}
		}	
		System.out.println("json result: " + object.toString());
		//真正运行的时候 写过期时间
		jedis.set("corre_rec_realtime_filter_result:" + userId + ":" + bookId + ":" + type, object.toString());
		//jedis.setex(key, seconds, value)
		
	}
	
	private Map<String, String> compute(List<String> historyBooks, String bookId, String type) {

		
		//test
		System.out.println("historybook.size()" + historyBooks.size());
		for (String book : historyBooks) {
			System.out.println(book);			
		}
		
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
		
		//################test
		
		System.out.println("offline result: ");
		for (String item: offLineResult) {
			System.out.println(item);
		}
		//####################
		//从离线推荐结果中去掉用户阅读记录	
		
		
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
		//################test
		System.out.println("final result: ");
		for (Entry<String, String>  entry : selectedBookMap.entrySet()) {
			System.out.println(entry);
		}
		//###############test
		
		for (Entry<String, String> entry : backupBookMap.entrySet()) {
			if (selectedBookMap.size() >= recNumber) {
				break;				
			}		
			selectedBookMap.put(entry.getKey(), entry.getValue());			
		} 
		return selectedBookMap;	
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
