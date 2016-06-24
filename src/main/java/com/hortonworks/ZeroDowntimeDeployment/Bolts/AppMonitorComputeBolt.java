package com.hortonworks.ZeroDowntimeDeployment.Bolts;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hortonworks.ZeroDowntimeDeployment.Utils.AppMonitor;
import com.hortonworks.ZeroDowntimeDeployment.Utils.FieldNames;
import com.hortonworks.ZeroDowntimeDeployment.Utils.Helper;
import com.hortonworks.ZeroDowntimeDeployment.Utils.SolrDate;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AppMonitorComputeBolt extends BaseRichBolt {


	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	private double mean;
	private double std;
	private Map<AppMonitor, Double> appRate;
	SimpleDateFormat parseDate;
	
	@Override
	public void execute(Tuple tuple) {

		if (tuple.getFields().get(0).equals(FieldNames.COMMANDCOMPUTE)) {

			computerZscore();

		} else {

			String host = tuple.getStringByField(FieldNames.HOST);
			String module = tuple.getStringByField(FieldNames.MODULE);
			String version = tuple.getStringByField(FieldNames.VERSION);
			double avgResponseInfo = tuple.getDoubleByField(FieldNames.AVGRESPONSEINFO);

			AppMonitor appMonitor = new AppMonitor(host, module, version);
			appRate.put(appMonitor, avgResponseInfo);

		}

		collector.ack(tuple);
	}

	private void computerZscore() {

		List<Double> rateList = new ArrayList<>(appRate.values());

		double curMean = Helper.getMean(rateList);
		double curStd = Helper.getStd(rateList, curMean);

		double stdInUse = std;
		double meanInUse = mean;

		if (!Helper.isValidStd(stdInUse)) {
			stdInUse = curStd;
			meanInUse = curMean;
		}

		if (!Helper.isValidStd(stdInUse)) {

			Set<Map.Entry<AppMonitor, Double>> outputSet = appRate.entrySet();
			Iterator<Map.Entry<AppMonitor, Double>> outputIt = outputSet
					.iterator();
			while (outputIt.hasNext()) {
				Map.Entry<AppMonitor, Double> outputEntry = outputIt.next();
				AppMonitor outputAppMonitor = outputEntry.getKey();
				Date date = new Date();
				String dateString = SolrDate.getSolrDate(parseDate.format(date));
				collector.emit(new Values(outputAppMonitor.getHost(),
						outputAppMonitor.getModule(), outputAppMonitor
								.getVersion(), outputEntry.getValue(), 0, dateString));
				
			}

		} else {

			Set<Map.Entry<AppMonitor, Double>> outputSet = appRate.entrySet();
			Iterator<Map.Entry<AppMonitor, Double>> outputIt = outputSet
					.iterator();
			while (outputIt.hasNext()) {
				Map.Entry<AppMonitor, Double> outputEntry = outputIt.next();
				AppMonitor outputAppMonitor = outputEntry.getKey();

				double zscore = (outputEntry.getValue() - meanInUse) / stdInUse;

				Date date = new Date();
				String dateString = SolrDate.getSolrDate(parseDate.format(date));
				
				collector.emit(new Values(outputAppMonitor.getHost(),
						outputAppMonitor.getModule(), outputAppMonitor
								.getVersion(), outputEntry.getValue(), zscore, dateString));
				
				//System.out.println(outputEntry.toString() + ":" + zscore);
			}
		}

		mean = curMean;
		std = curStd;
		appRate.clear();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.mean = 0;
		this.std = 0;
		this.appRate = new HashMap<>();
		this.parseDate = new SimpleDateFormat("yyyy-MM-dd'T':HH:mm:ssZ");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.HOST, FieldNames.MODULE,
				FieldNames.VERSION, FieldNames.AVGRESPONSEINFO, FieldNames.ZSCORE, FieldNames.PROCESSTIME));
	}

}

