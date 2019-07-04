package org.hswebframework.lettuce;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;

public interface LettucePlus {

    <K, V> CompletionStage<StatefulRedisConnection<K, V>> getConnection(RedisCodec<K, V> codec, Duration timeout);

    <K, V> CompletionStage<StatefulRedisConnection<K, V>> getConnection(Duration timeout);

    <K, V> CompletionStage<StatefulRedisConnection<K, V>> getConnection();

    RedisEventBus getEventBus(String id);

    RedisHaManager getHaManager(String id);

    <T> RedisTopic<T> getTopic(String name);

    <T> RedisQueue<T> getQueue(String id);

    ScheduledExecutorService getExecutor();
}
