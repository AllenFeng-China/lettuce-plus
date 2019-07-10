package org.jetlinks.lettuce;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;

public interface LettucePlus {

    <K, V> RedisCodec<K, V> getDefaultCodec();

    <K, V> CompletionStage<RedisAsyncCommands<K, V>> getRedisAsync(RedisCodec<K, V> codec);

    <K, V> CompletionStage<StatefulRedisConnection<K, V>> getConnection();

    default <T> CompletionStage<T> eval(String script, ScriptOutputType type, Object[] keys, Object... values) {
        return eval(script, type, getDefaultCodec(), keys, values);
    }

    default <T> CompletionStage<T> evalReadOnly(String script, ScriptOutputType type, Object[] keys, Object... values) {
        return evalReadOnly(script, type, getDefaultCodec(), keys, values);
    }

    <K, V, T> CompletionStage<T> eval(String script, ScriptOutputType type, RedisCodec<K, V> codec, K[] keys, V... values);

    <K, V, T> CompletionStage<T> evalReadOnly(String script, ScriptOutputType type, RedisCodec<K, V> codec, K[] keys, V... values);

    RedisHaManager getHaManager(String id);

    <T> RedisTopic<T> getTopic(String name);

    <T> RedisTopic<T> getTopic(RedisCodec<String, T> codec, String name);

    <T> RedisTopic<T> getPatternTopic(RedisCodec<String, T> codec, String name);

    <T> RedisTopic<T> getPatternTopic(String name);

    <T> RedisQueue<T> getQueue(String id);

    <T> RedisQueue<T> getQueue(RedisCodec<String, T> codec, String id);


    <K, V> RedisLocalCacheMap<K, V> getLocalCacheMap(String id);

    ScheduledExecutorService getExecutor();
}
