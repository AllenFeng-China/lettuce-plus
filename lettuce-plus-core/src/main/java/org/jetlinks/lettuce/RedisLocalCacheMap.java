package org.jetlinks.lettuce;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;

public interface RedisLocalCacheMap<K,V> extends ConcurrentMap<K,V> {

    CompletionStage<Void> fastPutAsync(K key, V value);

    CompletionStage<V> putAsync(K key, V value);

    CompletionStage<Void> fastRemoveAsync(K key);

    CompletionStage<Void> fastRemoveAllAsync(K... key);

}
