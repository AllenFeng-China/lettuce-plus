package org.hswebframework.lettuce;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

public interface RedisHaManager {

    String getId();

    ServerNodeInfo getCurrentNode();

    List<ServerNodeInfo> getAllNode();

    void onNodeJoin(Consumer<ServerNodeInfo> consumer);

    void onNodeLeave(Consumer<ServerNodeInfo> consumer);

    <T> CompletionStage<T> sendNotifyReply(String serverId, String address, Object payload, Duration timeout);

    CompletionStage<Boolean> sendNotify(String serverId, String address, Object payload);

    <T> void onNotify(String address, Class<T> type, Consumer<T> listener);

    <T> void onNotify(String address, Class<T> type, Function<T, CompletionStage<?>> listener);

    void startup(ServerNodeInfo current);

    void shutdown();

}
