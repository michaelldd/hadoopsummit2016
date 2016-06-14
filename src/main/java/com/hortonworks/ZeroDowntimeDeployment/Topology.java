package com.hortonworks.ZeroDowntimeDeployment;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.hortonworks.ZeroDowntimeDeployment.Bolts.AppMonitorBolt;
import com.hortonworks.ZeroDowntimeDeployment.Spouts.CommandComputeSpout;
import com.hortonworks.ZeroDowntimeDeployment.Spouts.LocalAppSpout;

public class Topology {
	
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		//builder.setSpout("LocalAccessSpout", new LocalAccessSpout(), 1);
		builder.setSpout("LocalAppSpout", new LocalAppSpout(), 1);
		builder.setSpout("CommandComputeSpout", new CommandComputeSpout(), 1);
		
		builder.setBolt("AppMonitorBolt", new AppMonitorBolt(), 1)
		.globalGrouping("LocalAppSpout")
		.allGrouping("CommandComputeSpout");
		
		
		Config conf = new Config();
		conf.setDebug(false);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(20);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("ZeroDowntimeDeployment", conf, builder.createTopology());
			Utils.sleep(20000);
			cluster.killTopology("ZeroDowntimeDeployment");
			cluster.shutdown();
		}
	}
}
