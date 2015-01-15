package realtime.filter.filter;

import java.util.Map;

import redis.clients.jedis.Jedis;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RequestSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private Jedis jedis;//是不是同一个实例


	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;	
		this.jedis = new Jedis("10.1.70.4", 3030);
	}

	@Override
	public void nextTuple() {
		//collector.emit(new Values("user1", "book1","1"));
		//collector.emit(new Values("user1", "book1","2"));
		collector.emit(new Values("user1", "book1","3"));
		try {
			Thread.sleep(1000 * 600);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user", "book", "type"));
	}

}
