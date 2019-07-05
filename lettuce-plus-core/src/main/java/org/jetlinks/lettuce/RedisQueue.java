package org.jetlinks.lettuce;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public interface RedisQueue<T> {

    void poll(Consumer<T> listener);

    CompletionStage<T> poll();

    void removeListener(Consumer<T> listener);

    CompletionStage<Boolean> addAll(Collection<T> data);

    CompletionStage<Boolean> addAsync(T data);

}
