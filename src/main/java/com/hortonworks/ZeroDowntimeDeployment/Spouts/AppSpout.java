package com.hortonworks.ZeroDowntimeDeployment.Spouts;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;

import com.hortonworks.ZeroDowntimeDeployment.Utils.Configs;

public class AppSpout {

	private KafkaSpout kafkaSpout;
	
	public AppSpout() {
		kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());
	}

	public KafkaSpout getSpout() {
		return this.kafkaSpout;
	}
	
	private SpoutConfig constructKafkaSpoutConf() {
		
		BrokerHosts hosts = new ZkHosts(Configs.kafka_zookeeper_host_port);
		String topic = Configs.kafka_topic_app_cloud;
		String zkRoot = Configs.kafka_zkRoot_app_cloud;
		String consumerGroupId = Configs.kafka_zkRoot_consumerGroupId_cloud;

		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot,
				consumerGroupId);

		spoutConfig.scheme = new SchemeAsMultiScheme(new AppLogScheme());

		return spoutConfig;
	}
	
}