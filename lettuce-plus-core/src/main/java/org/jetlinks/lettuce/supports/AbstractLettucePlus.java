package org.jetlinks.lettuce.supports;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.lettuce.*;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

@SuppressWarnings("all")
public abstract class AbstractLettucePlus implements LettucePlus {

    @Getter
    @Setter
    protected RedisCodec<?, ?> defaultCodec;

    @Setter
    protected ScheduledExecutorService executorService;

    private final Map<String, RedisHaManager> haManagerMap = new ConcurrentHashMap<>();

    private final Map<String, DefaultRedisTopic> redisTopicMap = new ConcurrentHashMap<>();
    private final Map<String, DefaultRedisQueue> queueMap = new ConcurrentHashMap<>();

    public abstract <K, V> CompletionStage<StatefulRedisConnection<K, V>> getConnection(RedisCodec<K, V> codec, Duration timeout);

    @Override
    public <K, V> RedisCodec<K, V> getDefaultCodec() {
        return (RedisCodec)defaultCodec;
    }

    @SuppressWarnings("all")
    public <K, V> CompletionStage<StatefulRedisConnection<K, V>> getConnection(Duration timeout) {
        return getConnection((RedisCodec<K, V>) defaultCodec, timeout);
    }

    @Override
    @SuppressWarnings("all")
    public <K, V> CompletionStage<StatefulRedisConnection<K, V>> getConnection() {
        return getConnection((RedisCodec<K, V>) defaultCodec, Duration.ofSeconds(60));
    }

    protected abstract <K, V> CompletionStage<StatefulRedisPubSubConnection<K, V>> getPubSubConnect();

    @Override
    public RedisHaManager getHaManager(String id) {
        return haManagerMap.computeIfAbsent(id, _id -> new DefaultRedisHaManager(_id, this));
    }

    @Override
    public ScheduledExecutorService getExecutor() {
        return executorService;
    }

    @Override
    public <T> RedisTopic<T> getTopic(String name) {

        return redisTopicMap.computeIfAbsent(name, _name -> {

            AtomicBoolean first = new AtomicBoolean(true);

            return new DefaultRedisTopic<T>() {

                @Override
                public void addListener(BiConsumer<String, T> listener) {
                    if (first.get()) {
                        subscripe(_name);
                    }
                    first.set(false);
                    super.addListener(listener);
                }

                @Override
                public CompletionStage<Long> publish(T data) {
                    return getConnection()
                            .thenCompose(connection -> connection.async().publish(_name, data));
                }

                @Override
                public void shutdown() {
                    super.shutdown();
                    unsubscripe(_name);
                    first.set(true);
                }
            };
        });
    }


    @Override
    public <T> RedisQueue<T> getQueue(String id) {
        return queueMap.computeIfAbsent(id, _id -> new DefaultRedisQueue(_id, this));
    }

    protected void init() {
        executorService.scheduleAtFixedRate(() -> {
            for (DefaultRedisQueue value : queueMap.values()) {
                value.flush();
            }
        }, 10, 5, TimeUnit.SECONDS);
    }


    private RedisPubSubListener<String, Object> listener = new RedisPubSubAdapter<String, Object>() {

        @Override
        public void message(String channel, Object message) {
            Optional.ofNullable(redisTopicMap.get(channel))
                    .ifPresent(topic -> topic.onMessage(channel, message));
        }

        @Override
        public void message(String pattern, String channel, Object message) {
            Optional.ofNullable(redisTopicMap.get(pattern))
                    .ifPresent(topic -> topic.onMessage(channel, message));
        }

        @Override
        public void subscribed(String channel, long count) {
            super.subscribed(channel, count);
        }
    };

    protected void bindPubSubListener(StatefulRedisPubSubConnection<String, Object> connection) {
        connection.removeListener(listener);
        connection.addListener(listener);
    }

    protected abstract void subscripe(String topic);

    protected abstract void unsubscripe(String topic);

}
