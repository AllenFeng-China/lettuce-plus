package org.hswebframework.lettuce.supports;

import org.hswebframework.lettuce.RedisTopic;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

abstract class DefaultRedisTopic<T> implements RedisTopic<T> {

    private List<BiConsumer<String, T>> listeners = new CopyOnWriteArrayList<>();

    private String topic;

    @Override
    public void addListener(BiConsumer<String, T> listener) {
        listeners.add(listener);
    }

    void onMessage(String topic,T message) {
        listeners.forEach(listener -> listener.accept(topic, message));
    }

    @Override
    public abstract CompletionStage<Long> publish(T data);

    @Override
    public void shutdown() {
        listeners.clear();
    }
}
