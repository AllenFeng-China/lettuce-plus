package org.hswebframework.lettuce;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

public interface RedisEventBus {

    CompletionStage<Long> publish(String address, Object event);

    <T> CompletionStage<T> publishReply(String address, Object event);

    <T> long addListener(String address, Function<T, CompletionStage<Object>> listener);

    <T> long addListener(String address, Consumer<T> listener);

    void removeListener(String address, long id);

    void removeListener(String address, Object listener);

}
