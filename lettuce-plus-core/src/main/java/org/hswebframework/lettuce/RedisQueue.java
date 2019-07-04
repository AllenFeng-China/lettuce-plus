package org.hswebframework.lettuce;

import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public interface RedisQueue<T>  {

    void poll(Consumer<T> listener);

    CompletionStage<T> poll();

    void removeListener(Consumer<T> listener);

    CompletionStage<Boolean> addAsync(T data);


}
