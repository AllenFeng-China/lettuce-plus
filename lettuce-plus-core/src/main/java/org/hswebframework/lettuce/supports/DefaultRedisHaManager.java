package org.hswebframework.lettuce.supports;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hswebframework.lettuce.LettucePlus;
import org.hswebframework.lettuce.RedisHaManager;
import org.hswebframework.lettuce.RedisTopic;
import org.hswebframework.lettuce.ServerNodeInfo;

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

    private RedisTopic<Event> eventTopic;

    private RedisTopic<EventReply> replyTopic;

    private RedisTopic<ServerNodeInfo> broadcastTopic;

    private Map<String, NotifyReply<?>> replyMap = new ConcurrentHashMap<>();

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

        Event event = Event.of(UUID.randomUUID().toString(), address, payload.getClass().getName(), payload);

        NotifyReply<T> reply = new NotifyReply<>();
        reply.timeout = System.currentTimeMillis() + timeout.toMillis();

        replyMap.put(event.getEventId(), reply);

        //同一个服务器节点
        if (serverId.equals(getCurrentNode().getId())) {
            handleEvent(event);
        } else {
            doSend(event).whenComplete((number, error) -> {
                if (number != null && number <= 0) {
                    replyMap.remove(event.getEventId());
                    reply.future.complete(null);
                } else if (error != null) {
                    replyMap.remove(event.getEventId());
                    reply.future.completeExceptionally(error);
                }
            });
        }

        return reply.future;
    }

    public CompletionStage<Long> doSend(Event event) {

        return eventTopic.publish(event);
    }

    @Override
    public CompletionStage<Boolean> sendNotify(String serverId, String address, Object payload) {
        Event event = Event.of(UUID.randomUUID().toString(), address, payload.getClass().getName(), payload);
        if (serverId.equals(getCurrentNode().getId())) {
            handleEvent(event);
            return CompletableFuture.completedFuture(true);
        }
        return doSend(event)
                .thenApply(number -> number != null && number > 0);

    }

    private void handleEvent(Event event) {
        Optional.ofNullable(notifyListener.get(event.getAddress()))
                .ifPresent(list -> list.forEach(listener -> listener.acceptMessage(event)));
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
    public synchronized void startup(ServerNodeInfo current) {
        if (running) {
            return;
        }
        running = true;
        current.setId(id);
        current.setState(ServerNodeInfo.State.ONLINE);

        this.current = current;

        onNotify("ping", String.class, (Function<String, CompletionStage<?>>) CompletableFuture::completedFuture);

        this.eventTopic = plus.getTopic("__ha-manager-notify:".concat(id));
        this.replyTopic = plus.getTopic("__ha-manager-notify-reply:".concat(id));
        //节点上下线广播
        this.broadcastTopic = plus.getTopic("__ha-manager-broadcast:".concat(id));
        this.broadcastTopic.publish(current);

        this.broadcastTopic.addListener((topic, node) -> {
            if (node.getId().equals(getCurrentNode().getId())) {
                return;
            }
            if (node.getState() == ServerNodeInfo.State.ONLINE) {
                if (null == allNodeInfo.put(node.getId(), node)) {
                    log.info("ha manager:[{}] node [{}] join", id, node.getId());
                    for (Consumer<ServerNodeInfo> listener : this.joinListeners) {
                        listener.accept(node);
                    }
                }
            } else {
                if (null != allNodeInfo.remove(node.getId())) {
                    log.info("ha manager:[{}] node [{}] leave", id, node.getId());
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
        getAllNode().forEach(server -> sendNotifyReply(server.getId(), "ping", "pong", Duration.ofSeconds(10))
                .whenComplete((pong, error) -> {
                    if (error != null) {
                        log.warn("ping server[{}] error", server.getId(), error);
                        return;
                    }
                    if (!"pong".equals(pong)) {
                        allNodeInfo.remove(server.getId());
                        server.setState(ServerNodeInfo.State.OFFLINE);
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
    public  void shutdown() {
        for (Runnable runnable : shutdownHooks) {
            runnable.run();
        }
        shutdownHooks.clear();
        running = false;
    }

    private interface NotifyListener {

        void acceptMessage(Event message);

    }

    @AllArgsConstructor
    private class ConsumerListener<T> implements NotifyListener {

        private Consumer<T> consumer;
        private Class<T> type;

        @Override
        public void acceptMessage(Event message) {
            consumer.accept(convertEvent(type, message));
        }
    }

    @AllArgsConstructor
    private class ReplyListener<T> implements NotifyListener {

        private Function<T, CompletionStage<?>> function;

        private Class<T> type;

        @Override
        public void acceptMessage(Event message) {
            try {
                function.apply(convertEvent(type, message))
                        .whenComplete((reply, error) -> replyTopic.publish(EventReply.of(message, reply, error)));
            } catch (Throwable e) {
                replyTopic.publish(EventReply.of(message, null, e));
            }
        }
    }

    @SuppressWarnings("all")
    protected <T> T convertEvent(Class<T> type, Event event) {

        return (T) event.getPayload();
    }

    private class NotifyReply<T> {
        CompletableFuture<T> future = new CompletableFuture<>();
        long timeout;
    }
}
