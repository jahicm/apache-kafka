package org.kafka.stream.three;

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
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.kafka.stream.clients.producer.MockDataProducer;
import org.kafka.stream.model.Purchase;
import org.kafka.stream.model.PurchasePattern;
import org.kafka.stream.model.RewardAccumulator;
import org.kafka.stream.utils.StreamsSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamThree {

	private static final String TRANSACTIONS = "transactions";
	private static final String PATTERNS = "patterns";
	private static final String REWARDS = "rewards";
	private static final String PURCHASES = "purchases";
	private static final String STATE_STORE_NAME = "POINTS_STORE_REWARDS";
	private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamThree.class);

	public void start() throws InterruptedException {

		Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
		Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
		Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
		Serde<String> stringSerde = Serdes.String();

		StreamsBuilder streamsBuilder = new StreamsBuilder();

		// Read from transactions topic , mask the credit card number and produce
		// Purchase Stream with the Key assigned
		KStream<String, Purchase> purchaseKStream = streamsBuilder
				.stream(TRANSACTIONS, Consumed.with(stringSerde, purchaseSerde))
				.mapValues(p -> Purchase.builder(p).maskCreditCard().build());

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

		// Repartition
		String rewardsStateStoreName = STATE_STORE_NAME;
		//Create Store in memory supplier and build it
		KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(rewardsStateStoreName);
		StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier,
				Serdes.String(), Serdes.Integer());

		streamsBuilder.addStateStore(storeBuilder);
		//Create key bases on Card Number
		KeyValueMapper<String, Purchase, String> purchaseCardAsKey = (key, purchase) -> purchase.getCreditCardNumber();
		Repartitioned<String, Purchase> repartitioned = Repartitioned.with(stringSerde, purchaseSerde);
		//Create repartitioned KStream by passing key and Repartition object (instead of using depricated through method)
		KStream<String, Purchase> customerStream = purchaseKStream.selectKey(purchaseCardAsKey)
				.repartition(repartitioned);
		//Save all accumulated reward points to the state and stream results back to rewards TOPIC
		KStream<String, RewardAccumulator> statefulRewardAccumulator = customerStream
				.transformValues(() -> new PurchaseRewardTransformer(rewardsStateStoreName), rewardsStateStoreName);

		statefulRewardAccumulator.print(Printed.<String, RewardAccumulator>toSysOut().withLabel(REWARDS));
		statefulRewardAccumulator.to(REWARDS, Produced.with(stringSerde, rewardAccumulatorSerde));
		//

		
		
		
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