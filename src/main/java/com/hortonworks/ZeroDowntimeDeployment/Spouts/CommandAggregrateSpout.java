package com.hortonworks.ZeroDowntimeDeployment.Spouts;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.hortonworks.ZeroDowntimeDeployment.Utils.FieldNames;

public class CommandAggregrateSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	
	@Override
	public void nextTuple() {
		Utils.sleep(5000);
		collector.emit(new Values(FieldNames.COMMANDAGGREGRATE));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldNames.COMMANDAGGREGRATE));
	}

}
