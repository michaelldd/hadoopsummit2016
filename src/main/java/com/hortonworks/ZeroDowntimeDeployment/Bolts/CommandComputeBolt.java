package com.hortonworks.ZeroDowntimeDeployment.Bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.hortonworks.ZeroDowntimeDeployment.Utils.FieldNames;

public class CommandComputeBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	@Override
	public void execute(Tuple tuple) {
		if (tuple.getFields().get(0).equals(FieldNames.COMMANDAGGREGRATE)) {
			Utils.sleep(1000);
			collector.emit(new Values(FieldNames.COMMANDCOMPUTE));
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.COMMANDCOMPUTE));
	}

}
