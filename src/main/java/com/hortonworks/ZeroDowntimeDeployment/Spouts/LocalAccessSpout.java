package com.hortonworks.ZeroDowntimeDeployment.Spouts;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.hortonworks.ZeroDowntimeDeployment.Utils.FieldNames;

public class LocalAccessSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	SpoutOutputCollector collector;

	List<String> accessLogs;
	int accessLogIdx;
	Random random;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map map, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		accessLogs = new ArrayList<>();
		accessLogIdx = 0;
		random = new Random();
		
		getLogs("acclog_dev.log", accessLogs);
	}

	private void getLogs(String fileName, List<String> logs) {

		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource(fileName).getFile());

		try (Scanner scanner = new Scanner(file)) {

			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				logs.add(line);
			}

			scanner.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void nextTuple() {
		
		Utils.sleep(500);
		
		String log = null;
		
		if(accessLogIdx < accessLogs.size()) {
			log = accessLogs.get(accessLogIdx);
			accessLogIdx += 1;
		} else {
			log = accessLogs.get(random.nextInt(accessLogs.size()));
		}
		//System.out.println("LOG:" + log);
		
		String[] parts = log.split("\\|");
		//String datetime = parts[0];
		String host = parts[1];
		String module = parts[2];
		String version = parts[3];
		String method = parts[4];
		//String endpoint = parts[5];
		String responseCode = parts[6];
		String latency = parts[7];
		//String content = parts[8];
		
		collector.emit(new Values(host, module, version, method, responseCode, latency));
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields(
				FieldNames.HOST,
				FieldNames.MODULE,
				FieldNames.VERSION,
				FieldNames.METHOD,
				FieldNames.RESPONSECODE,
				FieldNames.LATENCY
		));

	}

}
