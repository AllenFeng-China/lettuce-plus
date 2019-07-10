package org.jetlinks.lettuce.supports;

import io.lettuce.core.RedisAsyncCommandsImpl;
import io.lettuce.core.RedisException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.*;
import io.lettuce.core.protocol.*;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.lettuce.*;

import java.nio.ByteBuffer;
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

    protected final Map<String, RedisHaManager> haManagerMap = new ConcurrentHashMap<>();

    protected final Map<String, DefaultRedisTopic> redisTopicMap = new ConcurrentHashMap<>();

    protected final Map<String, DefaultRedisQueue> queueMap = new ConcurrentHashMap<>();

    protected final Map<String, DefaultRedisLocalCacheMap> localCacheMap = new ConcurrentHashMap<>();

    public <K, V> CompletionStage<RedisAsyncCommands<K, V>> getRedisAsync(RedisCodec<K, V> codec) {
        if (codec == defaultCodec) {
            return this.<K, V>getConnection().thenApply(StatefulRedisConnection::async);
        }
        return this.<K, V>getConnection()
                .thenApply(conn -> {
                    return new RedisAsyncCommandsImpl<>(conn, codec);
                });
    }

    @Override
    public <K, V> RedisCodec<K, V> getDefaultCodec() {
        return (RedisCodec) defaultCodec;
    }

    @Override
    public RedisHaManager getHaManager(String id) {
        return haManagerMap.computeIfAbsent(id, _id -> new DefaultRedisHaManager(_id, this));
    }

    @Override
    public <K, V, T> CompletionStage<T> eval(String script, ScriptOutputType type, RedisCodec<K, V> codec, K[] keys, V... values) {
        return doEval(WriteEvalCommand.EVAL, script, type, codec, keys, values);
    }

    @Override
    public <K, V, T> CompletionStage<T> evalReadOnly(String script, ScriptOutputType type, RedisCodec<K, V> codec, K[] keys, V... values) {

        return doEval(CommandType.EVAL, script, type, codec, keys, values);
    }

    public <K, V, T> CompletionStage<T> doEval(ProtocolKeyword keyword, String script, ScriptOutputType type, RedisCodec<K, V> codec, K[] keys, V... values) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);

        args.add(script.getBytes()).add(keys.length).addKeys(keys).addValues(values);

        AsyncCommand<K, V, T> command = new AsyncCommand<>(new Command<>(keyword, newScriptOutput(codec, type), args));

        this.<K, V>getConnection()
                .thenApply(conn -> conn.dispatch(command))
                .whenComplete((_com, error) -> {
                    if (error != null) {
                        command.completeExceptionally(error);
                    }
                });

        return command;
    }

    @SuppressWarnings("unchecked")
    protected <K, V, T> CommandOutput<K, V, T> newScriptOutput(RedisCodec<K, V> codec, ScriptOutputType type) {
        switch (type) {
            case BOOLEAN:
                return (CommandOutput<K, V, T>) new BooleanOutput<K, V>(codec);
            case INTEGER:
                return (CommandOutput<K, V, T>) new IntegerOutput<K, V>(codec);
            case STATUS:
                return (CommandOutput<K, V, T>) new StatusOutput<K, V>(codec);
            case MULTI:
                return (CommandOutput<K, V, T>) new NestedMultiOutput<K, V>(codec);
            case VALUE:
                return (CommandOutput<K, V, T>) new ValueOutput<K, V>(codec);
            default:
                throw new RedisException("Unsupported script output type");
        }
    }

    @Override
    public ScheduledExecutorService getExecutor() {
        return executorService;
    }

    @Override
    public <T> RedisTopic<T> getTopic(RedisCodec<String, T> codec, String name) {

        return redisTopicMap.computeIfAbsent(name, _name -> {

            AtomicBoolean first = new AtomicBoolean(true);

            return new DefaultRedisTopic<T>() {

                @Override
                public RedisCodec<String, T> getCodec() {
                    return codec;
                }

                @Override
                public boolean isPattern() {
                    return false;
                }

                @Override
                public void addListener(BiConsumer<String, T> listener) {
                    if (first.get()) {
                        subscripe(_name, false);
                    }
                    first.set(false);
                    super.addListener(listener);
                }

                @Override
                public CompletionStage<Long> publish(T data) {
                    return getRedisAsync(codec)
                            .thenCompose(redis -> redis.publish(_name, data));
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
    public <T> RedisTopic<T> getTopic(String name) {
        return getTopic(getDefaultCodec(), name);
    }

    @Override
    public <T> RedisTopic<T> getPatternTopic(RedisCodec<String, T> codec, String name) {

        return redisTopicMap.computeIfAbsent(name, _name -> {

            AtomicBoolean first = new AtomicBoolean(true);

            return new DefaultRedisTopic<T>() {

                @Override
                public RedisCodec<String, T> getCodec() {
                    return codec;
                }

                @Override
                public boolean isPattern() {
                    return true;
                }

                @Override
                public void addListener(BiConsumer<String, T> listener) {
                    if (first.get()) {
                        subscripe(_name, true);
                    }
                    first.set(false);
                    super.addListener(listener);
                }

                @Override
                public CompletionStage<Long> publish(T data) {
                    return getRedisAsync(codec)
                            .thenCompose(redis -> redis.publish(_name, data));
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

    public <T> RedisTopic<T> getPatternTopic(String name) {
        return getPatternTopic(this.getDefaultCodec(), name);
    }

    @Override
    public <K, V> RedisLocalCacheMap<K, V> getLocalCacheMap(String id) {
        return localCacheMap.computeIfAbsent(id, _id -> {
            return new DefaultRedisLocalCacheMap(_id, this);
        });
    }

    @Override
    public <T> RedisQueue<T> getQueue(String id) {
        return getQueue(getDefaultCodec(), id);
    }

    @Override
    public <T> RedisQueue<T> getQueue(RedisCodec<String, T> codec, String id) {
        return queueMap.computeIfAbsent(id, _id -> new DefaultRedisQueue(_id, this, codec));
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
                    .ifPresent(topic -> {
                        Object msg = message;
                        if (msg instanceof ByteBuffer) {
                            msg = topic.getCodec().decodeValue(((ByteBuffer) msg));
                        }
                        Object _msg = msg;
                        getExecutor().execute(() -> {
                            topic.onMessage(channel, _msg);
                        });

                    });
        }

        @Override
        public void message(String pattern, String channel, Object message) {
            Optional.ofNullable(redisTopicMap.get(pattern))
                    .ifPresent(topic -> {
                        Object msg = message;
                        if (msg instanceof ByteBuffer) {
                            msg = topic.getCodec().decodeValue(((ByteBuffer) msg));
                        }
                        Object _msg = msg;
                        getExecutor().execute(() -> {
                            topic.onMessage(channel, _msg);
                        });
                    });
        }

        @Override
        public void subscribed(String channel, long count) {
            super.subscribed(channel, count);
        }

        @Override
        public void psubscribed(String pattern, long count) {
            super.psubscribed(pattern, count);
        }
    };

    protected void bindPubSubListener(StatefulRedisPubSubConnection<String, Object> connection) {
        connection.removeListener(listener);
        connection.addListener(listener);
    }

    protected abstract void subscripe(String topic, boolean pattern);

    protected abstract void unsubscripe(String topic);

    enum WriteEvalCommand implements ProtocolKeyword {
        EVAL;

        private final byte[] name;

        WriteEvalCommand() {
            name = name().getBytes();
        }

        @Override
        public byte[] getBytes() {
            return name;
        }
    }


}
