package com.hortonworks.ZeroDowntimeDeployment.Bolts;

import java.util.Map;

import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.hortonworks.ZeroDowntimeDeployment.Utils.Configs;
import com.hortonworks.ZeroDowntimeDeployment.Utils.FieldNames;

@SuppressWarnings("deprecation")
public class AppMonitorSolrBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private HttpSolrServer server;
	OutputCollector collector;
	
	@Override
	public void execute(Tuple tuple) {
		
		String host = tuple.getStringByField(FieldNames.HOST);
		String module = tuple.getStringByField(FieldNames.MODULE);
		String version = tuple.getStringByField(FieldNames.VERSION);
		double avgResponseInfo = tuple.getDoubleByField(FieldNames.AVGRESPONSEINFO);
		double zscore = tuple.getDoubleByField(FieldNames.ZSCORE);
		String processTime = tuple.getStringByField(FieldNames.PROCESSTIME);
		
		
		
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", "app-" + host + "-" + module + "-" + version);
		doc.addField("avgresponseinfo", avgResponseInfo);
		doc.addField("zscore", zscore);
		doc.addField("processtime", processTime);
		doc.addField( "event_timestamp", new java.util.Date(), 1.0f);

		try {
			server.add(doc);
			server.commit(false, false);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		collector.ack(tuple);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.server =  new HttpSolrServer(Configs.solr_server_cloud);
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}