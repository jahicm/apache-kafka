package org.kafka.stream;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kafka.stream.model.Purchase;
import org.kafka.stream.model.PurchasePattern;
import org.kafka.stream.model.RewardAccumulator;
import org.kafka.stream.utils.StreamsSerdes;
import org.kafka.stream.utils.datagen.DataGenerator;

public class KafkaApplicationTests {

	private TopologyTestDriver topologyTestDriver;

	@BeforeEach
	public void setUp() {

		Properties props = new Properties();
		props.put(StreamsConfig.CLIENT_ID_CONFIG, "Kafka-Streams-Client");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "purchases");
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Kafka-Streams-App");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

		Topology topology = KafkaTopology.build();

		topologyTestDriver = new TopologyTestDriver(topology, props);

	}

	@Test
	void test() {

		Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
		Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
		Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
		Serde<String> stringSerde = Serdes.String();

		TestInputTopic<String, Purchase> inputTopic = topologyTestDriver.createInputTopic("transactions",
				stringSerde.serializer(), purchaseSerde.serializer());
		inputTopic.pipeInput(null, DataGenerator.generatePurchase());

		TestOutputTopic<String, Purchase> outputTopic = topologyTestDriver.createOutputTopic("purchases",
				stringSerde.deserializer(), purchaseSerde.deserializer());
		Purchase purchase = DataGenerator.generatePurchase();

		ProducerRecord<String, Purchase> record = topologyTestDriver.readOutput("purchases", stringSerde.deserializer(),
				purchaseSerde.deserializer());

		Purchase expectedPurchase = Purchase.builder(purchase).maskCreditCard().build();

		RewardAccumulator expectedRewardAccumulator = RewardAccumulator.builder(expectedPurchase).build();

		ProducerRecord<String, RewardAccumulator> accumulatorProducerRecord = topologyTestDriver.readOutput("rewards",
				stringSerde.deserializer(), rewardAccumulatorSerde.deserializer());

		PurchasePattern expectedPurchasePattern = PurchasePattern.builder(expectedPurchase).build();

		ProducerRecord<String, PurchasePattern> purchasePatternProducerRecord = topologyTestDriver
				.readOutput("patterns", stringSerde.deserializer(), purchasePatternSerde.deserializer());

		assertNotNull(record);
		assertNotNull(accumulatorProducerRecord);
		assertNotNull(purchasePatternProducerRecord);
	}

}
