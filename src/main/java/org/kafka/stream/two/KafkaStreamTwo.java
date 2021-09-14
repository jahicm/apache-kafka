package org.kafka.stream.two;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.kafka.stream.clients.producer.MockDataProducer;
import org.kafka.stream.model.Purchase;
import org.kafka.stream.model.PurchasePattern;
import org.kafka.stream.model.RewardAccumulator;
import org.kafka.stream.utils.StreamsSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamTwo {

	private static final String TRANSACTIONS = "transactions";
	private static final String PATTERNS = "patterns";
	private static final String REWARDS = "rewards";
	private static final String PURCHASES = "purchases";
	private final int TOTAL_REWARDS_LIMIT = 50;
	private static final String HIGH_POINTS = "high-points-topic";
	private static final String LOW_POINTS = "low-points-topic";

	private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamTwo.class);

	public void start() throws InterruptedException {

		Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
		Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
		Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
		Serde<String> stringSerde = Serdes.String();

		StreamsBuilder streamsBuilder = new StreamsBuilder();

		// Create key from Credit Card Number value
		KeyValueMapper<String, Purchase, String> purchaseCardAsKey = (key, purchase) -> purchase.getCreditCardNumber();

		// Read from transactions topic , mask the credit card number and produce
		// Purchase Stream with the Key assigned
		KStream<String, Purchase> purchaseKStream = streamsBuilder
				.stream(TRANSACTIONS, Consumed.with(stringSerde, purchaseSerde))
				.mapValues(p -> Purchase.builder(p).maskCreditCard().build()).selectKey(purchaseCardAsKey);

		// Take Purchase Stream and produce Purchase Pattern stream
		KStream<String, PurchasePattern> patternKStream = purchaseKStream
				.mapValues(purchase -> PurchasePattern.builder(purchase).build());

		// Print the Purchase Pattern stream in the console and write the data to
		// Patterns topic
		patternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel(PATTERNS));
		patternKStream.to(PATTERNS, Produced.with(stringSerde, purchasePatternSerde));

		// Take Purchase stream and produce Rewards Stream
		KStream<String, RewardAccumulator> rewardsKStream = purchaseKStream
				.mapValues(purchase -> RewardAccumulator.builder(purchase).build());

		// Print Rewards data in the console and write the rewards data to the Rewards
		// topic
		rewardsKStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel(REWARDS));
		rewardsKStream.to(REWARDS, Produced.with(stringSerde, rewardAccumulatorSerde));

		// Print Purchases data in the console and write the purchase data to the
		// Purchases topic
		purchaseKStream.print(Printed.<String, Purchase>toSysOut().withLabel(PURCHASES));
		purchaseKStream.to(PURCHASES, Produced.with(stringSerde, purchaseSerde));

		////// Branch to high and low topics based on rewards///////
		////// points (topics created on the fly)///////

		Predicate<String, RewardAccumulator> highPoints = (key,
				rewards) -> rewards.getTotalRewardPoints() >= TOTAL_REWARDS_LIMIT;// Create predicate for clients with
																					// >=50 points
		Predicate<String, RewardAccumulator> lowPoints = (key,
				rewards) -> rewards.getTotalRewardPoints() < TOTAL_REWARDS_LIMIT;// Create predicate for clients with
																					// <50 points
		// Create branch with 2 streams, lower and higher points
		KStream<String, RewardAccumulator>[] kstreamByTotalRewards = rewardsKStream.branch(highPoints, lowPoints);

		// Assign index 0 for higher points and print to high-points topic
		kstreamByTotalRewards[0].to(HIGH_POINTS, Produced.with(stringSerde, rewardAccumulatorSerde));
		kstreamByTotalRewards[0].print(Printed.<String, RewardAccumulator>toSysOut().withLabel(HIGH_POINTS));
		// Assign index 1 for lower points and print to lower-points topic
		kstreamByTotalRewards[1].to(LOW_POINTS, Produced.with(stringSerde, rewardAccumulatorSerde));
		kstreamByTotalRewards[1].print(Printed.<String, RewardAccumulator>toSysOut().withLabel(LOW_POINTS));
		//////////////////////////////////////////////////////////////////////////////////////////////
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
