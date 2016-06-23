package com.hortonworks.ZeroDowntimeDeployment.Utils;

public class Configs {
	public static final String kafka_zookeeper_host_port = "nn1-wwang.cloud.hortonworks.com:2181";

	public static final String kafka_topic_access = "acclog-simulation";
	public static final String kafka_topic_app = "applog-simulation";
	public static final String kafka_zkRoot_access = "/hadoopsummit2016/access";
	public static final String kafka_zkRoot_app = "/hadoopsummit2016/app";
	public static final String kafka_zkRoot_consumerGroupId = "simulation";
	
	public static final String kafka_topic_access_cloud = "accesslogs";
	public static final String kafka_topic_app_cloud = "applicationlogs";
	public static final String kafka_zkRoot_access_cloud = "/hadoopsummit2016cloud/access";
	public static final String kafka_zkRoot_app_cloud = "/hadoopsummit2016cloud/app";
	public static final String kafka_zkRoot_consumerGroupId_cloud = "deployanalysis";
	
	public static final String metaStoreURI = "thrift://rm-wwang.cloud.hortonworks.com:9083";
	public static final String dbName = "default";
	public static final String tblName_hostresponse="hostresponse";
	public static final String tblName_hostlatancy="hostlatancy";
	public static final String tblName_appmonitor="appmonitor";
	public static final String solr_server="http://localhost:8983/solr/gettingstarted";
	public static final String solr_server_cloud="http://gwy-wwang.cloud.hortonworks.com:8983/solr/test1_shard1_replica1";
}
