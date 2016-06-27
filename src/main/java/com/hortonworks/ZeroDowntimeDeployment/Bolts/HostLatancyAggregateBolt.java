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

public class HostLatancyAggregateBolt  extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	private Map<String, List<Double>> latancyMap;
	
	@Override
	public void execute(Tuple tuple) {
		
		if (tuple.getFields().get(0).equals(FieldNames.COMMANDAGGREGRATE)) {

			aggregrate();
			latancyMap.clear();

		} else {

			String host = tuple.getStringByField(FieldNames.HOST);
			if(host.equals(FieldNames.BADRECORD)){
				collector.ack(tuple);
				return;
			}
			
			double latancy;
			
			try{
				latancy = Double.parseDouble(tuple.getStringByField(FieldNames.LATENCY));
			} catch(Exception e) {
				System.out.println("Latancy exception:");
				System.out.println(tuple.getStringByField(FieldNames.LATENCY));
				System.out.println(e);
				return;
			}
			
			
			List<Double> latancyList = null;

			if (!latancyMap.containsKey(host)) {
				latancyMap.put(host, new ArrayList<Double>());
			}
			latancyList = latancyMap.get(host);

			latancyList.add(latancy);
		}
		
		collector.ack(tuple);
	}

	private void aggregrate() {
		
		Set<Map.Entry<String, List<Double>>> set = latancyMap.entrySet();
		Iterator<Map.Entry<String, List<Double>>> it = set.iterator();
		
		while(it.hasNext()) {
			Map.Entry<String, List<Double>> entry = it.next();
			String host = entry.getKey();
			List<Double> list = entry.getValue();
			
			double total = 0;
			for(Double d : list) {
				total += d;
			}
			
			double tmpRate = 0;
			
			if(list.size() != 0){
				tmpRate = total / list.size();	
			}
			
			
			//System.out.println("HostLatancy:" + host + ":" + list.size() + ":" + tmpRate);
			collector.emit(new Values(host, tmpRate));
		}
		
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.latancyMap = new HashMap<>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.HOST, FieldNames.AVGLATANCY));
	}

}
