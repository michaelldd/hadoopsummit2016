package com.hortonworks.ZeroDowntimeDeployment;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.hortonworks.ZeroDowntimeDeployment.Spouts.LocalAccessSpout;

public class Topology {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		
		/*
		builder.setSpout("LocalAppSpout", new LocalAppSpout(), 1);
		builder.setSpout("CommandAggregrateSpout", new CommandAggregrateSpout(), 1);
		
		builder.setBolt("CommandComputedBolt", new CommandComputeBolt(), 1)
		.allGrouping("CommandAggregrateSpout");
		
		builder.setBolt("AppMonitorAggregateBolt", new AppMonitorAggregateBolt(), 3)
		.fieldsGrouping("LocalAppSpout", new Fields(FieldNames.HOST, FieldNames.MODULE, FieldNames.VERSION))
		.allGrouping("CommandAggregrateSpout");
		
		builder.setBolt("AppMonitorComputeBolt", new AppMonitorComputeBolt(), 1)
		.allGrouping("CommandComputedBolt")
		.allGrouping("AppMonitorAggregateBolt");
		*/

		builder.setSpout("LocalAccessSpout", new LocalAccessSpout(), 1);

		
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
