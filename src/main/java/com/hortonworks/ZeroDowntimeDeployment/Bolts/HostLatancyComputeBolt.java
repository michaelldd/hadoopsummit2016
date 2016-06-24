package com.hortonworks.ZeroDowntimeDeployment.Bolts;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.hortonworks.ZeroDowntimeDeployment.Utils.FieldNames;
import com.hortonworks.ZeroDowntimeDeployment.Utils.Helper;
import com.hortonworks.ZeroDowntimeDeployment.Utils.SolrDate;

public class HostLatancyComputeBolt extends BaseRichBolt {


	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	private double mean;
	private double std;
	private Map<String, Double> latancyRate;
	SimpleDateFormat parseDate;
	
	@Override
	public void execute(Tuple tuple) {

		if (tuple.getFields().get(0).equals(FieldNames.COMMANDCOMPUTE)) {

			computerZscore();

		} else {

			String host = tuple.getStringByField(FieldNames.HOST);
			double latancy = tuple.getDoubleByField(FieldNames.AVGLATANCY);

			latancyRate.put(host, latancy);

		}

		collector.ack(tuple);
	}

	private void computerZscore() {

		List<Double> latancyList = new ArrayList<>(latancyRate.values());

		double curMean = Helper.getMean(latancyList);
		double curStd = Helper.getStd(latancyList, curMean);

		double stdInUse = std;
		double meanInUse = mean;

		if (!Helper.isValidStd(stdInUse)) {
			stdInUse = curStd;
			meanInUse = curMean;
		}

		if (!Helper.isValidStd(stdInUse)) {

			Set<Map.Entry<String, Double>> outputSet = latancyRate.entrySet();
			Iterator<Map.Entry<String, Double>> outputIt = outputSet
					.iterator();
			while (outputIt.hasNext()) {
				Map.Entry<String, Double> outputEntry = outputIt.next();
				String host = outputEntry.getKey();
				Date date = new Date();
				String dateString = SolrDate.getSolrDate(parseDate.format(date));
				collector.emit(new Values(host, outputEntry.getValue(), 0, dateString));
			}

		} else {

			Set<Map.Entry<String, Double>> outputSet = latancyRate.entrySet();
			Iterator<Map.Entry<String, Double>> outputIt = outputSet
					.iterator();
			while (outputIt.hasNext()) {
				Map.Entry<String, Double> outputEntry = outputIt.next();
				String host = outputEntry.getKey();

				double zscore = (outputEntry.getValue() - meanInUse) / stdInUse;
				Date date = new Date();
				String dateString = SolrDate.getSolrDate(parseDate.format(date));
				collector.emit(new Values(host, outputEntry.getValue(), zscore, dateString));
				
				//System.out.println("latancyRate:" + host + ":" + outputEntry.getValue() + ":zscore:" + zscore);
			}
		}

		mean = curMean;
		std = curStd;
		latancyRate.clear();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.mean = 0;
		this.std = 0;
		this.latancyRate = new HashMap<>();
		
		this.parseDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.HOST, FieldNames.AVGLATANCY, FieldNames.ZSCORE, FieldNames.PROCESSTIME));
	}

}

