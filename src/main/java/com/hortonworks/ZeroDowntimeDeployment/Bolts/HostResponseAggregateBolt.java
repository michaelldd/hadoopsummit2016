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

import com.hortonworks.ZeroDowntimeDeployment.Utils.FieldNames;

public class HostResponseAggregateBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	private Map<String, List<Integer>> responseMap;
	
	@Override
	public void execute(Tuple tuple) {
		
		if (tuple.getFields().get(0).equals(FieldNames.COMMANDAGGREGRATE)) {

			aggregrate();
			responseMap.clear();

		} else {

			String host = tuple.getStringByField(FieldNames.HOST);
			String responseCode = tuple
					.getStringByField(FieldNames.RESPONSECODE);

			List<Integer> responseList = null;

			if (!responseMap.containsKey(host)) {
				responseMap.put(host, new ArrayList<Integer>());
			}
			responseList = responseMap.get(host);

			if (responseCode.trim().equals("200")) {
				responseList.add(1);
			} else {
				responseList.add(0);
			}

		}
		
	}

	private void aggregrate() {
		
		Set<Map.Entry<String, List<Integer>>> set = responseMap.entrySet();
		Iterator<Map.Entry<String, List<Integer>>> it = set.iterator();
		
		while(it.hasNext()) {
			Map.Entry<String, List<Integer>> entry = it.next();
			String host = entry.getKey();
			List<Integer> list = entry.getValue();
			
			int total = 0;
			for(Integer i : list) {
				total += i;
			}
			double tmpRate = (double)total / list.size();
			
			//System.out.println("HostResponse:" + host + ":" + list.size() + ":" + tmpRate);
			collector.emit(new Values(host, tmpRate));
		}
		
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.responseMap = new HashMap<>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.HOST, FieldNames.AVGRESPONSECODE));
	}

}
