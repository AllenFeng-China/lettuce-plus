package org.jetlinks.lettuce.supports;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.RedisHaManager;
import org.jetlinks.lettuce.RedisTopic;
import org.jetlinks.lettuce.ServerNodeInfo;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public class DefaultRedisHaManager implements RedisHaManager {
    private String id;

    private volatile ServerNodeInfo current;

    private LettucePlus plus;

    private List<Consumer<ServerNodeInfo>> joinListeners = new CopyOnWriteArrayList<>();

    private List<Consumer<ServerNodeInfo>> leaveListeners = new CopyOnWriteArrayList<>();

    private Map<String, List<NotifyListener>> notifyListener = new ConcurrentHashMap<>();

    private RedisTopic<Notify> eventTopic;

    private RedisTopic<org.jetlinks.lettuce.supports.NotifyReply> replyTopic;

    private RedisTopic<ServerNodeInfo> broadcastTopic;

    private Map<String, NotifyReply> replyMap = new ConcurrentHashMap<>();

    private Map<String, ServerNodeInfo> allNodeInfo = new ConcurrentHashMap<>();

    private List<Runnable> shutdownHooks = new CopyOnWriteArrayList<>();

    @Override
    public List<ServerNodeInfo> getAllNode() {
        return new ArrayList<>(allNodeInfo.values());
    }

    public DefaultRedisHaManager(String id, LettucePlus plus) {
        this.id = id;
        this.plus = plus;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public ServerNodeInfo getCurrentNode() {
        if (current == null) {
            throw new IllegalArgumentException("please startup first");
        }
        return current;
    }

    @Override
    public void onNodeJoin(Consumer<ServerNodeInfo> consumer) {
        joinListeners.add(consumer);
    }

    @Override
    public void onNodeLeave(Consumer<ServerNodeInfo> consumer) {
        leaveListeners.add(consumer);
    }

    @Override
    public <T> CompletionStage<T> sendNotifyReply(String serverId, String address, Object payload, Duration timeout) {

        Notify notify = Notify.of(current.getId(), UUID.randomUUID().toString(), address, payload.getClass().getName(), payload);

        NotifyReply<T> reply = new NotifyReply<>();
        reply.timeout = System.currentTimeMillis() + timeout.toMillis();
        replyMap.put(notify.getNotifyId(), reply);

        //同一个服务器节点
        if (serverId.equals(getCurrentNode().getId())) {
            handleEvent(notify);
        } else {
            doSend(serverId, notify)
                    .whenComplete((number, error) -> {
                        if (number != null && number <= 0) {
                            replyMap.remove(notify.getNotifyId());
                            reply.future.complete(null);
                        } else if (error != null) {
                            replyMap.remove(notify.getNotifyId());
                            reply.future.completeExceptionally(error);
                        }
                    });
        }

        return reply.future;
    }

    public CompletionStage<Long> doSend(String server, Notify notify) {

        return plus.getTopic("__ha-manager-notify:".concat(id).concat(":").concat(server))
                .publish(notify);
    }

    @Override
    public CompletionStage<Boolean> sendNotify(String serverId, String address, Object payload) {
        Notify notify = Notify.of(current.getId(), UUID.randomUUID().toString(), address, payload.getClass().getName(), payload);
        if (serverId.equals(getCurrentNode().getId())) {
            handleEvent(notify);
            return CompletableFuture.completedFuture(true);
        }
        return doSend(serverId, notify)
                .thenApply(number -> number != null && number > 0);

    }

    private void handleEvent(Notify notify) {
        Optional.ofNullable(notifyListener.get(notify.getAddress()))
                .ifPresent(list -> list.forEach(listener -> listener.acceptMessage(notify)));
    }

    @Override
    public <T> void onNotify(String address, Class<T> type, Consumer<T> listener) {
        notifyListener.computeIfAbsent(address, _address -> new CopyOnWriteArrayList<>())
                .add(new ConsumerListener<>(listener, type));
    }

    @Override
    public <T> void onNotify(String address, Class<T> type, Function<T, CompletionStage<?>> listener) {
        notifyListener.computeIfAbsent(address, _address -> new CopyOnWriteArrayList<>())
                .add(new ReplyListener<>(listener, type));
    }

    private volatile boolean running = false;

    @Override
    @SuppressWarnings("all")
    public synchronized void startup(ServerNodeInfo current) {
        if (running) {
            return;
        }
        Objects.requireNonNull(current.getId());
        running = true;
        current.setState(ServerNodeInfo.State.ONLINE);

        plus.getConnection()
                .thenApply(StatefulRedisConnection::async)
                .thenAccept(commands -> {
                    commands.hgetall(((key, value) -> {
                        if (value instanceof ServerNodeInfo) {
                            ServerNodeInfo server = ((ServerNodeInfo) value);
                            if (!server.getId().equals(current.getId())) {
                                sendNotifyReply(server.getId(), "ping", "pong", Duration.ofSeconds(10))
                                        .thenAccept(pong -> {
                                            if ("pong".equals(pong)) {
                                                allNodeInfo.put(server.getId(), server);
                                            }
                                        });
                            }
                        } else {
                            log.warn("ServerNodeInfo data format error:{}={}", key, value);
                        }
                    }), "ha-manager:".concat(id));
                });

        this.current = current;
        allNodeInfo.put(this.current.getId(), current);
        onNotify("ping", String.class, (Function<String, CompletionStage<?>>) CompletableFuture::completedFuture);

        this.eventTopic = plus.getTopic("__ha-manager-notify:".concat(id).concat(":").concat(current.getId()));

        this.replyTopic = plus.getTopic("__ha-manager-notify-reply:".concat(id).concat(":").concat(current.getId()));
        //节点上下线广播
        this.broadcastTopic = plus.getTopic("__ha-manager-broadcast:".concat(id));
        this.broadcastTopic.publish(current);

        this.eventTopic.addListener((topic, notify) -> {
            handleEvent(notify);
        });

        this.replyTopic.addListener((topic, event) ->
                Optional.ofNullable(replyMap.remove(event.getNotifyId()))
                        .ifPresent(notifyReply -> {
                            if (event.isSuccess()) {
                                notifyReply.future.complete(event.getPayload());
                            } else {
                                notifyReply.future.completeExceptionally(new RuntimeException(event.getErrorMessage()));
                            }
                        }));
        this.broadcastTopic.addListener((topic, node) -> {
            if (node.getId().equals(getCurrentNode().getId())) {
                return;
            }
            if (node.getState() == ServerNodeInfo.State.ONLINE) {
                if (null == allNodeInfo.put(node.getId(), node)) {
                    log.info("server [{}] join [{}]", node.getId(), id);
                    for (Consumer<ServerNodeInfo> listener : this.joinListeners) {
                        listener.accept(node);
                    }
                }
            } else {
                if (null != allNodeInfo.remove(node.getId())) {
                    log.info("server [{}] leave [{}]", node.getId(), id);
                    for (Consumer<ServerNodeInfo> listener : this.leaveListeners) {
                        listener.accept(node);
                    }
                }
            }
        });
        ScheduledFuture<?> future = plus.getExecutor().scheduleAtFixedRate(this::checkServerAlive, 10, 10, TimeUnit.SECONDS);
        shutdownHooks.add(broadcastTopic::shutdown);
        shutdownHooks.add(replyTopic::shutdown);
        shutdownHooks.add(eventTopic::shutdown);
        shutdownHooks.add(() -> future.cancel(false));
    }

    private void checkServerAlive() {
        //上报自己
        plus.getConnection()
                .thenAccept(conn -> conn.async().hset("ha-manager:".concat(id), current.getId(), current));

        getAllNode()
                .stream()
                .filter(server -> !server.getId().equals(current.getId()))
                .forEach(server -> sendNotifyReply(server.getId(), "ping", "pong", Duration.ofSeconds(10))
                        .whenComplete((pong, error) -> {
                            if (error != null) {
                                log.warn("ping server[{}] error", server.getId(), error);
                                return;
                            }
                            if (!"pong".equals(pong)) {
                                server.setState(ServerNodeInfo.State.OFFLINE);

                                plus.getConnection()
                                        .thenAccept(conn -> conn.async().hdel("ha-manager:".concat(id), server.getId()));

                                if (allNodeInfo.remove(server.getId()) != null) {
                                    log.info("server [{}] leave [{}]", server.getId(), id);
                                    for (Consumer<ServerNodeInfo> listener : this.leaveListeners) {
                                        listener.accept(server);
                                    }
                                }
                                broadcastTopic.publish(server);
                            }
                        }));

        replyMap.entrySet()
                .stream()
                .filter(e -> System.currentTimeMillis() > e.getValue().timeout)
                .peek(e -> e.getValue().future.completeExceptionally(new TimeoutException()))
                .map(Map.Entry::getKey)
                .forEach(replyMap::remove);
    }


    @Override
    public void shutdown() {
        for (Runnable runnable : shutdownHooks) {
            runnable.run();
        }
        //移除自己
        plus.getConnection()
                .thenAccept(conn -> conn.async().hdel("ha-manager:".concat(id), current.getId()));

        current.setState(ServerNodeInfo.State.OFFLINE);
        broadcastTopic.publish(current);
        shutdownHooks.clear();
        running = false;
    }

    private interface NotifyListener {

        void acceptMessage(Notify message);

    }

    @AllArgsConstructor
    private class ConsumerListener<T> implements NotifyListener {

        private Consumer<T> consumer;
        private Class<T> type;

        @Override
        public void acceptMessage(Notify message) {
            consumer.accept(convertEvent(type, message));
        }
    }

    @AllArgsConstructor
    private class ReplyListener<T> implements NotifyListener {

        private Function<T, CompletionStage<?>> function;

        private Class<T> type;

        @Override
        @SuppressWarnings("all")
        public void acceptMessage(Notify message) {
            try {
                function.apply(convertEvent(type, message))
                        .whenComplete((reply, error) -> {
                            if (message.getFromServer().equals(current.getId())) {
                                NotifyReply localReply = replyMap.remove(message.getNotifyId());
                                if (localReply != null) {
                                    localReply.future.complete(reply);
                                }
                                return;
                            }
                            plus.getTopic("__ha-manager-notify-reply:".concat(id).concat(":").concat(message.getFromServer()))
                                    .publish(org.jetlinks.lettuce.supports.NotifyReply.of(message, reply, error));
                        });
            } catch (Throwable e) {
                replyTopic.publish(org.jetlinks.lettuce.supports.NotifyReply.of(message, null, e));
            }
        }
    }

    @SuppressWarnings("all")
    protected <T> T convertEvent(Class<T> type, Notify notify) {

        return (T) notify.getPayload();
    }

    private class NotifyReply<T> {
        CompletableFuture<T> future = new CompletableFuture<>();
        long timeout;
    }
}
