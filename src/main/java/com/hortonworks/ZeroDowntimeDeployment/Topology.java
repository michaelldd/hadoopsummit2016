package com.hortonworks.ZeroDowntimeDeployment;

import org.apache.storm.hive.bolt.HiveBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.hortonworks.ZeroDowntimeDeployment.Bolts.AppMonitorAggregateBolt;
import com.hortonworks.ZeroDowntimeDeployment.Bolts.AppMonitorComputeBolt;
import com.hortonworks.ZeroDowntimeDeployment.Bolts.CommandComputeBolt;
import com.hortonworks.ZeroDowntimeDeployment.Bolts.DemoHiveBolt;
import com.hortonworks.ZeroDowntimeDeployment.Bolts.HostLatancyAggregateBolt;
import com.hortonworks.ZeroDowntimeDeployment.Bolts.HostLatancyComputeBolt;
import com.hortonworks.ZeroDowntimeDeployment.Bolts.HostResponseAggregateBolt;
import com.hortonworks.ZeroDowntimeDeployment.Bolts.HostResponseComputeBolt;
import com.hortonworks.ZeroDowntimeDeployment.Spouts.AccessSpout;
import com.hortonworks.ZeroDowntimeDeployment.Spouts.CommandAggregrateSpout;
import com.hortonworks.ZeroDowntimeDeployment.Utils.Configs;
import com.hortonworks.ZeroDowntimeDeployment.Utils.FieldNames;

// storm jar hadoopsummit2016-0.0.1-SNAPSHOT.jar com.hortonworks.ZeroDowntimeDeployment.Topology test1

public class Topology {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("AccessSpout", (new AccessSpout()).getSpout());

		builder.setSpout("CommandAggregrateSpout",
				new CommandAggregrateSpout(), 1);
		builder.setBolt("CommandComputedBolt", new CommandComputeBolt(), 1)
				.allGrouping("CommandAggregrateSpout");

		// host response
		builder.setBolt("HostResponseAggregateBolt",
				new HostResponseAggregateBolt(), 3)
				.fieldsGrouping("AccessSpout", new Fields(FieldNames.HOST))
				.allGrouping("CommandAggregrateSpout");

		builder.setBolt("HostResponseComputeBolt",
				new HostResponseComputeBolt(), 1)
				.allGrouping("CommandComputedBolt")
				.allGrouping("HostResponseAggregateBolt");

		HiveBolt hostResponseHiveBolt = (new DemoHiveBolt(new String[] 
				{FieldNames.HOST, FieldNames.AVGRESPONSECODE, FieldNames.ZSCORE, FieldNames.PROCESSTIME}, 
				Configs.tblName_hostresponse)).getHiveBolt();
	    
		builder.setBolt("hostResponseHiveBolt", hostResponseHiveBolt, 1)
			.shuffleGrouping("HostResponseComputeBolt");

		// host latancy
		builder.setBolt("HostLatancyAggregateBolt", new HostLatancyAggregateBolt(), 3)
		.fieldsGrouping("AccessSpout", new Fields(FieldNames.HOST))
		.allGrouping("CommandAggregrateSpout");

		builder.setBolt("HostLatancyComputeBolt", new HostLatancyComputeBolt(), 1)
			.allGrouping("CommandComputedBolt")
			.allGrouping("HostLatancyAggregateBolt");
		
		HiveBolt hostLatancyHiveBolt = (new DemoHiveBolt(new String[] 
				{FieldNames.HOST, FieldNames.AVGLATANCY, FieldNames.ZSCORE, FieldNames.PROCESSTIME}, 
				Configs.tblName_hostlatancy)).getHiveBolt();
	    
		builder.setBolt("hostLatancyHiveBolt", hostLatancyHiveBolt, 1)
			.shuffleGrouping("HostLatancyComputeBolt");
		
		
		// app monitor
		builder.setBolt("AppMonitorAggregateBolt", new AppMonitorAggregateBolt(), 3)
		.fieldsGrouping("AppSpout", new Fields(FieldNames.HOST, FieldNames.MODULE, FieldNames.VERSION))
		.allGrouping("CommandAggregrateSpout");
	
		builder.setBolt("AppMonitorComputeBolt", new AppMonitorComputeBolt(), 1)
		.allGrouping("CommandComputedBolt")
		.allGrouping("AppMonitorAggregateBolt");
		
		HiveBolt appMonitorHiveBolt = (new DemoHiveBolt(new String[] 
				{FieldNames.HOST, FieldNames.MODULE,
				FieldNames.VERSION, FieldNames.AVGRESPONSEINFO, FieldNames.ZSCORE, FieldNames.PROCESSTIME},
				Configs.tblName_appmonitor)).getHiveBolt();
	    
		builder.setBolt("appMonitorHiveBolt", appMonitorHiveBolt, 1)
			.shuffleGrouping("AppMonitorComputeBolt");
		
		
		Config conf = new Config();
		conf.setDebug(false);

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
