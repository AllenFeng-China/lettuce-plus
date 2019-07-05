package org.jetlinks.lettuce.supports;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.RedisQueue;
import org.jetlinks.lettuce.RedisTopic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Slf4j
public class DefaultRedisQueue<T> implements RedisQueue<T> {

    private LettucePlus plus;

    private String id;

    private List<Consumer<T>> listeners = new CopyOnWriteArrayList<>();

    private RedisTopic<String> flushTopic;

    private Queue<T> localBuffer = new ConcurrentLinkedQueue<>();

    public DefaultRedisQueue(String id, LettucePlus plus) {
        this.id = id;
        this.plus = plus;
        flushTopic = plus.getTopic("_queue_flush:".concat(id));
    }

    private AtomicLong flushCounter = new AtomicLong();

    private long maxFlushThread = Math.min(2, Runtime.getRuntime().availableProcessors());

    @Override
    public void poll(Consumer<T> listener) {
        listeners.add(listener);
        if (listeners.size() == 1) {
            flushTopic.addListener((topic, _id) -> flush());
        }
        flush();
    }

    @Override
    public void removeListener(Consumer<T> listener) {
        listeners.remove(listener);
        if (listeners.isEmpty()) {
            flushTopic.shutdown();
        }
    }

    @Override
    public CompletionStage<T> poll() {
        return plus.<String, T>getConnection()
                .thenCompose(connection -> connection.async().lpop(id));
    }

    private void doFlush(RedisAsyncCommands<String, T> commands) {
        if (listeners.isEmpty()) {
            return;
        }
        commands.lpop(id)
                .whenComplete((data, error) -> {
                    if (data != null) {
                        for (Consumer<T> listener : listeners) {
                            listener.accept(data);
                        }
                        doFlush(commands);
                    }
                });
    }

    @SuppressWarnings("unchecked")
    public void flush() {
        if (listeners.isEmpty()) {
            return;
        }
        if (flushCounter.get() > maxFlushThread) {
            return;
        }
        flushCounter.incrementAndGet();
        plus.<String, T>getConnection()
                .thenApply(StatefulRedisConnection::async)
                .whenComplete((command, error) -> {
                    try {
                        if (command != null) {
                            if (listeners.isEmpty()) {
                                return;
                            }
                            flushBuffer(command);
                            doFlush(command);
                        }
                    } finally {
                        flushCounter.decrementAndGet();
                    }
                });
    }

    @SuppressWarnings("unchecked")
    private void flushBuffer(RedisAsyncCommands<String, T> redis) {
        if (!localBuffer.isEmpty()) {
            List<T> buffer = new ArrayList<>(localBuffer.size());
            for (T b = localBuffer.poll(); b != null; b = localBuffer.poll()) {
                buffer.add(b);
            }
            redis.lpush(id, (T[]) buffer.toArray())
                    .whenComplete((len, error) -> {
                        if (error != null) {
                            localBuffer.addAll(buffer);
                        } else {
                            flushTopic.publish(id);
                        }
                    });
        }
    }

    public CompletionStage<Boolean> addAll(Collection<T> data) {
        return plus.<String, Object>getConnection()
                .thenCompose(connection -> connection.async().lpush(id, data.toArray()))
                .whenComplete((len, error) -> {
                    if (error != null) {
                        log.error("add queue [{}] data error", id, error);
                        if (!listeners.isEmpty()) {
                            for (Consumer<T> listener : listeners) {
                                data.forEach(listener);
                            }
                        } else {
                            localBuffer.addAll(data);
                        }
                    } else {
                        flushTopic.publish(id);
                    }
                }).thenApply(len -> true);
    }

    @Override
    public CompletionStage<Boolean> addAsync(T data) {

        if (!listeners.isEmpty() && Math.random() < 0.5) {
            for (Consumer<T> listener : listeners) {
                listener.accept(data);
            }
            return CompletableFuture.completedFuture(true);
        }

        return plus.<String, T>getConnection()
                .thenCompose(connection -> {
                    flushBuffer(connection.async());
                    AsyncCommand<String, T, Long> command = PushCommand.EVAL.newCommand(id, data, plus.getDefaultCodec());
                    connection.dispatch(command);
                    return command;
                })
                .whenComplete((len, error) -> {
                    if (error != null) {
                        log.error("add queue [{}] data error", id, error);
                        if (!listeners.isEmpty()) {
                            for (Consumer<T> listener : listeners) {
                                listener.accept(data);
                            }
                        } else {
                            localBuffer.add(data);
                        }
                    }
                }).thenApply(len -> true);
    }

    enum PushCommand implements ProtocolKeyword {
        EVAL;
        private static final String script =
                "local val = redis.call('lpush',KEYS[1],ARGV[1]);"
                        + "redis.call('publish',KEYS[2],KEYS[1]);"
                        + "return val;";

        private final byte[] name;

        PushCommand() {
            name = name().getBytes();
        }

        @Override
        public byte[] getBytes() {
            return name;
        }

        <T> AsyncCommand<String, T, Long> newCommand(String id, T data, RedisCodec<String, T> codec) {
            return new AsyncCommand<>(new Command<>(PushCommand.EVAL,
                    new IntegerOutput<>(codec),
                    new CommandArgs<>(codec)
                            .add(script)
                            .add(2)
                            .addKeys(id, "_queue_flush:".concat(id))
                            .addValue(data)
            ));
        }
    }


}
