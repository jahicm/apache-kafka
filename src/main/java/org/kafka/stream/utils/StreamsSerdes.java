package org.kafka.stream.utils;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.kafka.stream.model.Purchase;
import org.kafka.stream.model.PurchaseKey;
import org.kafka.stream.model.PurchasePattern;
import org.kafka.stream.model.RewardAccumulator;

public class StreamsSerdes {

	public static Serde<PurchasePattern> PurchasePatternSerde() {
		return new PurchasePatternsSerde();
	}

	public static Serde<RewardAccumulator> RewardAccumulatorSerde() {
		return new RewardAccumulatorSerde();
	}

	public static Serde<Purchase> PurchaseSerde() {
		return new PurchaseSerde();
	}

	public static Serde<PurchaseKey> purchaseKeySerde() {
		return new PurchaseKeySerde();
	}

	public static final class PurchaseKeySerde extends WrapperSerde<PurchaseKey> {
		public PurchaseKeySerde() {
			super(new JsonSerializer<>(), new JsonDeserializer<>(PurchaseKey.class));
		}
	}

	public static final class PurchasePatternsSerde extends WrapperSerde<PurchasePattern> {
		public PurchasePatternsSerde() {
			super(new JsonSerializer<>(), new JsonDeserializer<>(PurchasePattern.class));
		}
	}

	public static final class RewardAccumulatorSerde extends WrapperSerde<RewardAccumulator> {
		public RewardAccumulatorSerde() {
			super(new JsonSerializer<>(), new JsonDeserializer<>(RewardAccumulator.class));
		}
	}

	public static final class PurchaseSerde extends WrapperSerde<Purchase> {
		public PurchaseSerde() {
			super(new JsonSerializer<>(), new JsonDeserializer<>(Purchase.class));
		}
	}

	private static class WrapperSerde<T> implements Serde<T> {

		private JsonSerializer<T> serializer;
		private JsonDeserializer<T> deserializer;

		WrapperSerde(JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
			this.serializer = serializer;
			this.deserializer = deserializer;
		}

		@Override
		public void configure(Map<String, ?> map, boolean b) {

		}

		@Override
		public void close() {

		}

		@Override
		public Serializer<T> serializer() {
			return serializer;
		}

		@Override
		public Deserializer<T> deserializer() {
			return deserializer;
		}
	}

}
