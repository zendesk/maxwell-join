package com.zendesk.maxwelljoin;

import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class KStreamMapKey<K, K1, V> implements ProcessorSupplier<K, V> {
	private final KeyMapper<K, K1, V> mapper;

	public KStreamMapKey(KeyMapper<K, K1, V> mapper) {
		this.mapper = mapper;
	}

	public Processor<K, V> get() {
		return new KStreamMapKeyProcessor();
	}

	private class KStreamMapKeyProcessor extends AbstractProcessor<K, V> {
		public void process(K key, V value) {
			Change<V> change = (Change<V>) value;
			K1 newKey = mapper.apply(key, change.newValue);
			context().forward(newKey, value);
		}
	}
}
