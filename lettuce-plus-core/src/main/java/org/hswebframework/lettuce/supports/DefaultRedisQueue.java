package org.hswebframework.lettuce.supports;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.lettuce.LettucePlus;
import org.hswebframework.lettuce.RedisQueue;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Slf4j
public class DefaultRedisQueue<T> implements RedisQueue<T> {

    private LettucePlus plus;

    private String id;

    private List<Consumer<T>> listeners = new CopyOnWriteArrayList<>();

    private volatile long lastPushFlush = System.currentTimeMillis();

    public DefaultRedisQueue(String id, LettucePlus plus) {
        this.id = id;
        this.plus = plus;
        plus.<String>getTopic("_queue_flush")
                .addListener((topic, _id) -> {
                    if (id.equals(_id)) {
                        flush();
                    }
                });
    }

    private AtomicLong flushCounter = new AtomicLong();

    private long maxFlushThread = Math.min(2, Runtime.getRuntime().availableProcessors() / 2);

    @Override
    public void poll(Consumer<T> listener) {
        listeners.add(listener);
        flush();
    }

    @Override
    public void removeListener(Consumer<T> listener) {
        listeners.remove(listener);
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
                            doFlush(command);
                        }
                    } finally {
                        flushCounter.decrementAndGet();
                    }
                });
    }

    @Override
    public CompletionStage<Boolean> addAsync(T data) {
        return plus.getConnection()
                .thenCompose(connection -> connection.async().lpush(id, data))
                .whenComplete((len, error) -> {
                    if (error == null && System.currentTimeMillis() - lastPushFlush > 100) {
                        plus.getTopic("_queue_flush").publish(id);
                        lastPushFlush = System.currentTimeMillis();
                    }
                }).thenApply(len -> true);
    }


}
