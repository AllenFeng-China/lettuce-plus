package org.hswebframework.lettuce;

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface RedisTopic<T> {

    void addListener(BiConsumer<String, T> listener);

    CompletionStage<Long> publish(T data);

    void shutdown();

}
