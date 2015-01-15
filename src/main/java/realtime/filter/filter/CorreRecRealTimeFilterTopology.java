package realtime.filter.filter;


import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;



public class CorreRecRealTimeFilterTopology {
	public static void main(String[] args) throws InterruptedException {
		
		//Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("request", new RequestSpout());
		builder.setBolt("filterbolt", new FilterBolt()).shuffleGrouping("request");
		
        //Configuration
		Config conf = new Config();	
		conf.setDebug(true);	
		
        //Topology run
		//conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());

		
	}

}
