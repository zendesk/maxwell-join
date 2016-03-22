package com.zendesk.maxwelljoin;

public interface KeyMapper<K, K1, V> {
	K1 apply(K key, V value);
}
