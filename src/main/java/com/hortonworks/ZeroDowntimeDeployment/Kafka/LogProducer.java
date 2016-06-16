package com.hortonworks.ZeroDowntimeDeployment.Kafka;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

// java -cp hadoopsummit2016-0.0.1-SNAPSHOT.jar com.hortonworkZeroDowntimeDeployment.Kafka.LogProducer gwy-wwang.cloud.hortonworks.com:6667 nn1-wwang.cloud.hortonworks.com:2181 acclog-simulation ./acclog_dev.log
// /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper nn1-wwang.cloud.hortonworks.com:2181 --topic acclog-simulation --from-beginning

// java -cp hadoopsummit2016-0.0.1-SNAPSHOT.jar com.hortonworkZeroDowntimeDeployment.Kafka.LogProducer gwy-wwang.cloud.hortonworks.com:6667 nn1-wwang.cloud.hortonworks.com:2181 applog-simulation ./applog_dev.log
// /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper nn1-wwang.cloud.hortonworks.com:2181 --topic applog-simulation --from-beginning
public class LogProducer {

	Random random;
	List<String> logs;

	public LogProducer(String inputFile) {
		random = new Random();
		logs = new ArrayList<>();
		getLogs(inputFile);
	}

	public static void main(String[] args) {

		if (args.length != 4) {
			System.out
					.println("Usage: LogProducer broker_list zookeeper topic inputFile");
			System.exit(-1);
		}

		Properties props = new Properties();
		props.put("metadata.broker.list", args[0]);
		props.put("zk.connect", args[1]);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");

		String TOPIC = args[2];
		String inputFile = args[3];

		LogProducer logProducer = new LogProducer(inputFile);

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

		while (true) {
			logProducer.sentKafkaSignal(producer, TOPIC);
		}

	}

	private void getLogs(String fileName) {

		// ClassLoader classLoader = getClass().getClassLoader();
		// File file = new File(classLoader.getResource(fileName).getFile());
		File file = new File(fileName);

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

	private void sentKafkaSignal(Producer<String, String> producer, String TOPIC) {

		// Utils.sleep(10);
		try {
			Thread.sleep(10); // 1000 milliseconds is one second.
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}

		String signal = logs.get(random.nextInt(logs.size()));

		KeyedMessage<String, String> message = new KeyedMessage<String, String>(
				TOPIC, signal);

		producer.send(message);

	}
}
