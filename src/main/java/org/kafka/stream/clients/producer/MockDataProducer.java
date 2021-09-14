package org.kafka.stream.clients.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kafka.stream.model.Purchase;
import org.kafka.stream.utils.datagen.DataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class MockDataProducer {

	private static final Logger LOG = LoggerFactory.getLogger(MockDataProducer.class);

	private static Producer<String, String> producer;
	private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
	private static ExecutorService executorService = Executors.newFixedThreadPool(1);
	private static Callback callback;
	private static final String TRANSACTIONS_TOPIC = "transactions";

	private static volatile boolean keepRunning = true;

	public static void producePurchaseData() {
		producePurchaseData(DataGenerator.DEFAULT_NUM_PURCHASES, DataGenerator.NUM_ITERATIONS,
				DataGenerator.NUMBER_UNIQUE_CUSTOMERS);
	}

	public static void producePurchaseData(int numberPurchases, int numberIterations, int numberCustomers) {
		Runnable generateTask = () -> {
			init();
			int counter = 0;
			while (counter++ < numberIterations && keepRunning) {
				List<Purchase> purchases = DataGenerator.generatePurchases(numberPurchases, numberCustomers);
				List<String> jsonValues = convertToJson(purchases);
				for (String value : jsonValues) {
					ProducerRecord<String, String> record = new ProducerRecord<>(TRANSACTIONS_TOPIC, null, value);
					producer.send(record, callback);
				}
				LOG.info("Record batch sent");
				try {
					Thread.sleep(6000);
				} catch (InterruptedException e) {
					Thread.interrupted();
				}
			}
			LOG.info("Done");

		};
		executorService.submit(generateTask);
	}

	public static void shutdown() {
		LOG.info("Shutting down data generation");
		keepRunning = false;

		if (executorService != null) {
			executorService.shutdownNow();
			executorService = null;
		}
		if (producer != null) {
			producer.close();
			producer = null;
		}

	}

	private static void init() {
		if (producer == null) {
			LOG.info("Initializing the producer");
			Properties properties = new Properties();
			properties.put("bootstrap.servers", "localhost:9092");
			properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			properties.put("acks", "1");
			properties.put("retries", "3");

			producer = new KafkaProducer<>(properties);

			callback = (metadata, exception) -> {
				if (exception != null) {
					exception.printStackTrace();
				}
			};
			LOG.info("Producer initialized");
		}
	}

	private static <T> List<String> convertToJson(List<T> generatedDataItems) {
		List<String> jsonList = new ArrayList<>();
		for (T generatedData : generatedDataItems) {
			jsonList.add(convertToJson(generatedData));
		}
		return jsonList;
	}

	private static <T> String convertToJson(T generatedDataItem) {
		return gson.toJson(generatedDataItem);
	}

}
