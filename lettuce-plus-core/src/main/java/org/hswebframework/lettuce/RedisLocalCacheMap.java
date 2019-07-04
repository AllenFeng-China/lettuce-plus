package org.hswebframework.lettuce;


import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public interface RedisLocalCacheMap<K, V> extends Map<K, V> {

    CompletionStage<Void> fastPut(K key, V value);

    CompletionStage<Void> fastPut(K key, V value, Duration duration);

    CompletionStage<Void> fastRemove(K... key);

    void expireLocalKey(String key, Duration duration);

    CompletionStage<V> put(K key, V value, Duration duration);

}
