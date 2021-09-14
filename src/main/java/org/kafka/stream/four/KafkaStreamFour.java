package org.kafka.stream.four;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.kafka.stream.clients.producer.MockDataProducer;
import org.kafka.stream.model.Purchase;
import org.kafka.stream.utils.StreamsSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamFour {

	private static final String TRANSACTIONS = "transactions";
	private static final String TOOLS = "Tools";
	private static final String BOOKS = "Books";
	private static final String MERGED_PURCHASE = "joined_purchase";

	private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamFour.class);

	public void start() throws InterruptedException {

		Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
		Serde<String> stringSerde = Serdes.String();

		StreamsBuilder streamsBuilder = new StreamsBuilder();

		// Create key from Credit Card Number value
		KeyValueMapper<String, Purchase, String> purchaseCardAsKey = (key, purchase) -> purchase.getCreditCardNumber();

		// Read from transactions topic , mask the credit card number and produce
		// Purchase Stream with the key = null
		KStream<String, Purchase> purchaseKStream = streamsBuilder
				.stream(TRANSACTIONS, Consumed.with(stringSerde, purchaseSerde))
				.mapValues(p -> Purchase.builder(p).maskCreditCard().build()).selectKey(purchaseCardAsKey);

		// Merge the branches
		Predicate<String, Purchase> toolsDpt = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase(TOOLS);
		Predicate<String, Purchase> booksDpt = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase(BOOKS);

		// Create branch with 2 streams, electronics and coffee with key
		KStream<String, Purchase>[] kstreamPurchases = purchaseKStream.branch(toolsDpt, booksDpt);

		int TOOLS_PURCHASE = 0;
		int BOOKS_PURCHASE = 1;

		KStream<String, Purchase> toolsStream = kstreamPurchases[TOOLS_PURCHASE];
		KStream<String, Purchase> booksStream = kstreamPurchases[BOOKS_PURCHASE];

		// Merge stream Tools and Books to new stream
		KStream<String, Purchase> mergedKStream = toolsStream.merge(booksStream);
		// Write stream to new topic MERGED_PURCHASE
		// mergedKStream.print(Printed.<String, Purchase>toSysOut().withLabel("Merged
		// KStream"));
		// mergedKStream.to(MERGED_PURCHASE, Produced.with(stringSerde, purchaseSerde));

		// Convert KStream to KTable
		KTable<String, Purchase> kTablePurchase = mergedKStream.toTable(Materialized.with(stringSerde, purchaseSerde));

		// Convert KTable back to KStream and print in the console and to the topic
		kTablePurchase.toStream().print(Printed.<String, Purchase>toSysOut().withLabel("Merged KStream"));
		kTablePurchase.toStream().to(MERGED_PURCHASE, Produced.with(stringSerde, purchaseSerde));

		MockDataProducer.producePurchaseData();
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), getProperties());
		LOG.info(" Kafka Streams Started");
		kafkaStreams.start();
		Thread.sleep(65000);
		LOG.info("Shutting down");
		kafkaStreams.close();
		MockDataProducer.shutdown();

	}

	private static Properties getProperties() {
		Properties props = new Properties();
		props.put(StreamsConfig.CLIENT_ID_CONFIG, "Kafka-Streams-Client");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "purchases");
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Kafka-Streams-App");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

		return props;
	}
}