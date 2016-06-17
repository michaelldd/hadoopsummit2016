package com.hortonworks.ZeroDowntimeDeployment.Bolts;

import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;

import backtype.storm.tuple.Fields;

import com.hortonworks.ZeroDowntimeDeployment.Utils.Configs;

/*
CREATE TABLE hostresponse(host STRING, avgresponsecode double, zscore double, processtime string)
 partitioned by (time string)
 clustered by (host) into 5 buckets
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
 stored as orc tblproperties("transactional"="true");

CREATE TABLE hostlatancy(host STRING, avglatancy double, zscore double, processtime string)
 partitioned by (time string)
 clustered by (host) into 5 buckets
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
 stored as orc tblproperties("transactional"="true");

CREATE TABLE appmonitor(host STRING, module string, version string, avgresponseinfo double, zscore double, processtime string)
 partitioned by (time string)
 clustered by (host) into 5 buckets
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
 stored as orc tblproperties("transactional"="true");


*/

public class DemoHiveBolt{

	private DelimitedRecordHiveMapper mapper;
	private HiveOptions hiveOptions;
	private HiveBolt hiveBolt;

	public DemoHiveBolt(String[] colNames, String tbName) {
		
		mapper = new DelimitedRecordHiveMapper()
				.withTimeAsPartitionField("yyyy/MM/dd/HH")
				.withColumnFields(new Fields(colNames));
		
		hiveOptions = new HiveOptions(Configs.metaStoreURI, Configs.dbName, tbName, mapper)
					.withTxnsPerBatch(10)
					.withBatchSize(100)
					.withIdleTimeout(10)
					.withMaxOpenConnections(1);

		hiveBolt = new HiveBolt(hiveOptions);
		
	}

	public HiveBolt getHiveBolt() {
		return hiveBolt;
	}
}
