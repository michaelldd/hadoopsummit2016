package com.hortonworks.ZeroDowntimeDeployment;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.hortonworks.ZeroDowntimeDeployment.Spouts.AccessSpout;

// storm jar hadoopsummit2016-0.0.1-SNAPSHOT.jar com.hortonworks.ZeroDowntimeDeployment.Topology test1

public class Topology {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("AccessSpout", (new AccessSpout()).getSpout());
		
		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(20);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("ZeroDowntimeDeployment", conf,
					builder.createTopology());
			Utils.sleep(30000);
			cluster.killTopology("ZeroDowntimeDeployment");
			cluster.shutdown();
		}
	}
}

