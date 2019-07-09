package org.jetlinks.lettuce;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;

public interface LettucePlus {

    <K, V>  RedisCodec<K, V> getDefaultCodec();

    <K, V> CompletionStage<StatefulRedisConnection<K, V>> getConnection(RedisCodec<K, V> codec, Duration timeout);

    <K, V> CompletionStage<StatefulRedisConnection<K, V>> getConnection();

    RedisHaManager getHaManager(String id);

    <T> RedisTopic<T> getTopic(String name);

    <T> RedisTopic<T> getPatternTopic(String name);

    <T> RedisQueue<T> getQueue(String id);

    <K,V>RedisLocalCacheMap<K,V> getLocalCacheMap(String id);

    ScheduledExecutorService getExecutor();
}
