package com.hortonworks.ZeroDowntimeDeployment.Bolts;

import java.util.ArrayList;
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

import com.hortonworks.ZeroDowntimeDeployment.Utils.AppMonitor;
import com.hortonworks.ZeroDowntimeDeployment.Utils.FieldNames;
import com.hortonworks.ZeroDowntimeDeployment.Utils.Helper;

public class AppMonitorBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;

	private Map<AppMonitor, List<Integer>> infoRates;
	private double mean;
	private double std;

	@Override
	public void execute(Tuple tuple) {

		if (tuple.getFields().get(0).equals(FieldNames.COMMANDCOMPUTE)) {

			aggregrateAndSubmit();

		} else {

			String host = tuple.getStringByField(FieldNames.HOST);
			String module = tuple.getStringByField(FieldNames.MODULE);
			String version = tuple.getStringByField(FieldNames.VERSION);
			String responseInfo = tuple
					.getStringByField(FieldNames.RESPONSEINFO);

			AppMonitor appMonitor = new AppMonitor(host, module, version);
			List<Integer> responseList = null;

			if (!infoRates.containsKey(appMonitor)) {
				infoRates.put(appMonitor, new ArrayList<Integer>());
			}
			responseList = infoRates.get(appMonitor);

			if (responseInfo.trim().equals("INFO")) {
				responseList.add(0);
			} else {
				responseList.add(1);
			}

		}
		collector.ack(tuple);
	}

	private void aggregrateAndSubmit() {

		Map<AppMonitor, Double> appRate = new HashMap<>();

		List<Double> rateList = new ArrayList<>();

		Set<Map.Entry<AppMonitor, List<Integer>>> set = infoRates.entrySet();
		Iterator<Map.Entry<AppMonitor, List<Integer>>> it = set.iterator();
		while (it.hasNext()) {
			Map.Entry<AppMonitor, List<Integer>> entry = it.next();
			List<Integer> list = entry.getValue();
			AppMonitor key = entry.getKey();
			
			int listTotal = 0;
			for (int i : list) {
				listTotal += i;
			}
			double tmpRate = (double)listTotal / list.size();

			System.out.println(key.toString() + ":" + list.size() + ":" + tmpRate);
			
			appRate.put(key, tmpRate);
			rateList.add(tmpRate);

		}

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
				collector.emit(new Values(outputAppMonitor.getHost(),
						outputAppMonitor.getModule(), outputAppMonitor
								.getVersion(), outputEntry.getValue(), 0));
				
			}

		} else {

			Set<Map.Entry<AppMonitor, Double>> outputSet = appRate.entrySet();
			Iterator<Map.Entry<AppMonitor, Double>> outputIt = outputSet
					.iterator();
			while (outputIt.hasNext()) {
				Map.Entry<AppMonitor, Double> outputEntry = outputIt.next();
				AppMonitor outputAppMonitor = outputEntry.getKey();

				double zscore = (outputEntry.getValue() - meanInUse) / stdInUse;

				collector.emit(new Values(outputAppMonitor.getHost(),
						outputAppMonitor.getModule(), outputAppMonitor
								.getVersion(), outputEntry.getValue(), zscore));
				
				System.out.println(outputEntry.toString() + ":" + zscore);
			}
		}

		mean = curMean;
		std = curStd;
		infoRates.clear();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		infoRates = new HashMap<>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.HOST, FieldNames.MODULE,
				FieldNames.VERSION, FieldNames.MEAN, FieldNames.ZSCORE));
	}

}
