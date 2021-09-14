package org.kafka.stream.one;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamOne {

	private Properties props;
	private Serde<String> stringSerde;
	private StreamsBuilder builder;
	private KStream<String, String> simpleFirstStream;
	private KafkaStreams kafkaStreams;
	private KStream<String, String> upperCasedStream;

	private static final String APP_ID = "myapp_id";
	private static final String SERVER = "localhost:9092";
	private static final String IN_TOPIC = "src-topic";
	private static final String OUT_TOPIC = "out-topic";
	private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamOne.class);

	public void start() throws InterruptedException {
		// Provide properties for application and connection details
		props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);

		// Create Kafka Serializable Deserializable object called Serde
		stringSerde = Serdes.String();

		// Create StreamBuilder for Kstream object
		builder = new StreamsBuilder();
		// Create KStream that consumes data from the src-topic using StreamBuilder and
		// "stream" method
		simpleFirstStream = builder.stream(IN_TOPIC, Consumed.with(stringSerde, stringSerde));

		// Create KStream that maps(modifies) original data from the topic using
		// previous KStream that loaded the data from the topic
		upperCasedStream = simpleFirstStream.mapValues(v -> "********" + v.toUpperCase() + "***********");
		
		// Once mapping is done, use method called "to" to write back to the out-topic ,
		// passing also Serde instances
		upperCasedStream.to(OUT_TOPIC, Produced.with(stringSerde, stringSerde));
		// Print also the modified data to the console
		upperCasedStream.print(Printed.<String, String>toSysOut().withLabel("Prinitng data"));

		// Open the new Kafka Stream by passing properties and StreamBuilder instance,
		// to listen to the topic
		kafkaStreams = new KafkaStreams(builder.build(), props);
		
		LOG.info("App Started");
		kafkaStreams.start();
		Thread.sleep(35000);
		LOG.info("Shutting down the APP now");

		kafkaStreams.close();
	}

}
