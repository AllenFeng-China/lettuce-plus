package org.jetlinks.lettuce.supports;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.RedisQueue;
import org.jetlinks.lettuce.RedisTopic;
import org.jetlinks.lettuce.codec.StringCodec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Slf4j
public class DefaultRedisQueue<T> implements RedisQueue<T> {

    private LettucePlus plus;

    private String id;

    private List<Consumer<T>> listeners = new CopyOnWriteArrayList<>();

    private RedisTopic<String> flushTopic;

    private Queue<T> localBuffer = new ConcurrentLinkedQueue<>();

    private double localConsumerPoint = Double.valueOf(System.getProperty("lettuce.plus.queue.local.consumer.point", "0.5"));

    private RedisCodec<String, T> codec;

    public DefaultRedisQueue(String id, LettucePlus plus, RedisCodec<String, T> codec) {
        this.id = id;
        this.plus = plus;
        this.codec = codec;
        flushTopic = plus.getTopic(StringCodec.getInstance(), "_queue_flush:".concat(id));
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
        if (!localBuffer.isEmpty()) {
            return CompletableFuture.completedFuture(localBuffer.poll());
        }
        return plus.getRedisAsync(codec)
                .thenCompose(redis -> redis.lpop(id));
    }

    private void doFlush(RedisAsyncCommands<String, T> commands) {
        if (listeners.isEmpty()) {
            return;
        }
        commands.lpop(id)
                .whenComplete((data, error) -> {
                    if (data != null) {
                        runListener(data, () -> doFlush(commands));

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
        plus.getRedisAsync(codec)
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

    private void flushBuffer() {
        plus.getRedisAsync(codec)
                .thenAccept(this::flushBuffer);
    }

    public CompletionStage<Boolean> addAll(Collection<T> data) {
        return doAdd(data)
                .whenComplete((len, error) -> {
                    if (error != null) {
                        log.error("add queue [{}] data error", id, error);
                        if (!listeners.isEmpty()) {
                            runListener(data);
                        } else {
                            localBuffer.addAll(data);
                        }
                    } else {
                        flushTopic.publish(id);
                    }
                });
    }

    protected CompletionStage<Boolean> doAdd(Collection<T> data) {
        return plus.getRedisAsync(codec)
                .thenCompose(redis -> redis.lpush(id, (T[]) data.toArray()))
                .thenApply(len -> true);
    }

    protected <K, V> RedisCodec<K, V> getCodec() {
        return ((RedisCodec) codec);
    }

    protected CompletionStage<Boolean> doAdd(T data) {
        return plus.<String,T,Long>eval("" +
                "local val = redis.call('lpush',KEYS[1],ARGV[1]);" +
                "redis.call('publish',KEYS[2],KEYS[1]);" +
                "return val;", ScriptOutputType.INTEGER, codec, new String[]{id, "_queue_flush:".concat(id)}, data)
                .thenApply(len -> true);
    }

    @Override
    public CompletionStage<Void> clear() {
        listeners.clear();
        return plus.getRedisAsync(plus.getDefaultCodec())
                .thenAccept(redis -> redis.del(id));
    }

    @Override
    public CompletionStage<Boolean> addAsync(T data) {

        if (!listeners.isEmpty() && Math.random() < localConsumerPoint) {
            runListener(data);
            return CompletableFuture.completedFuture(true);
        }

        return doAdd(data)
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
                    } else {
                        flushBuffer();
                    }
                });
    }

    public void runListener(Collection<T> data) {
        plus.getExecutor().execute(() -> {
            for (Consumer<T> listener : listeners) {
                data.forEach(listener);
            }
        });
    }

    public void runListener(T data) {
        runListener(data, null);
    }

    public void runListener(T data, Runnable runnable) {
        plus.getExecutor().execute(() -> {
            for (Consumer<T> listener : listeners) {
                listener.accept(data);
            }
            if (runnable != null) {
                runnable.run();
            }
        });
    }

}
