package realtime.filter.filter;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;



public class CorreRecRealTimeFilterTopology {
	public static void main(String[] args) throws InterruptedException {
		
		//Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("realtime-filter-request-spout", new RealTimeFilterRequestSpout() );
		builder.setBolt("realtime-fliter-bolt", new RealTimeFilterBolt(),100).shuffleGrouping("realtime-filter-request-spout");
		
        //Configuration
		Config conf = new Config();	
		conf.setDebug(false);	
		
        //Topology run
		//conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("corre-rec-realtime-filter-topology", conf, builder.createTopology());		
		
		
		try {
			StormSubmitter.submitTopology("corre-rec-realtime-filter-topology", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
