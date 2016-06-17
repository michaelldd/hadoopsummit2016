package com.hortonworks.ZeroDowntimeDeployment.Utils;

public class Configs {
	public static final String kafka_zookeeper_host_port = "nn1-wwang.cloud.hortonworks.com:2181";
	public static final String kafka_topic_access = "acclog-simulation";
	public static final String kafka_topic_app = "applog-simulation";
	public static final String kafka_zkRoot_access = "/mnt/access";
	public static final String kafka_zkRoot_app = "/mnt/app";
	public static final String metaStoreURI = "thrift://rm-wwang.cloud.hortonworks.com:9083";
	public static final String dbName = "default";
	public static final String tblName_hostresponse="hostresponse";
	public static final String tblName_hostlatancy="hostlatancy";
	public static final String tblName_appmonitor="appmonitor";
	
}
