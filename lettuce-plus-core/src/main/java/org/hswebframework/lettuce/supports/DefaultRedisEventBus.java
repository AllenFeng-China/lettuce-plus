package org.hswebframework.lettuce.supports;

import lombok.extern.slf4j.Slf4j;
import org.hswebframework.lettuce.LettucePlus;
import org.hswebframework.lettuce.RedisEventBus;
import org.hswebframework.lettuce.RedisTopic;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public class DefaultRedisEventBus implements RedisEventBus {

    private String id;

    private LettucePlus plus;

    private Map<String, List<Object>> listeners = new ConcurrentHashMap<>();

    private Map<Long, Object> idListener = new ConcurrentHashMap<>();

    private Map<String, Reply<?>> replyFutures = new ConcurrentHashMap<>();

    private RedisTopic<Event> eventTopic;

    private RedisTopic<EventReply> eventTopicReply;

    public DefaultRedisEventBus(String id, LettucePlus plus) {
        this.id = id;
        this.plus = plus;
        eventTopic = plus.getTopic("event-bus:".concat(id));

        eventTopicReply = plus.getTopic("event-bus-reply:".concat(id));

        eventTopic.addListener((topic, event) -> handleEvent(event));

        eventTopicReply.addListener((topic, reply) -> {
            Optional.ofNullable(replyFutures.remove(reply.getEventId()))
                    .ifPresent(r -> r.future.complete(convertEventPayload(reply)));
        });
    }

    void checkTimeout() {

    }

    @SuppressWarnings("all")
    private void handleEvent(Event event) {

        List<Object> list = listeners.get(event.getAddress());

        if (list == null || list.isEmpty()) {
            return;
        }

        Object payload = convertEventPayload(event);
        for (Object listener : list) {
            if (listener instanceof Consumer) {
                ((Consumer) listener).accept(payload);
            }
            //reply
            else if (listener instanceof Function) {
                CompletionStage<?> reply = (CompletionStage) ((Function) listener).apply(payload);
                reply.whenComplete((replyPayload, throwable) -> {
                    eventTopicReply.publish(EventReply.of(event, replyPayload, throwable));
                });
            } else {
                log.warn("unsupported listener type:{}", listener.getClass());
            }
        }
    }

    @SuppressWarnings("all")
    protected <T> T convertEventPayload(Event event) {

        return (T) event.getPayload();
    }

    @SuppressWarnings("all")
    protected <T> T convertEventPayload(EventReply event) {

        return (T) event.getPayload();
    }

    class Reply<T> {
        private long time = System.currentTimeMillis();
        CompletableFuture<T> future = new CompletableFuture<>();
    }

    @Override
    public CompletionStage<Long> publish(String address, Object event) {
        return doPublish(Event.of(generateEventId(), address, event.getClass().getName(), event));
    }

    private CompletionStage<Long> doPublish(Event event) {

        return eventTopic.publish(event);
    }

    protected String generateEventId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public <T> CompletionStage<T> publishReply(String address, Object event) {
        Reply<T> reply = new Reply<>();
        String eventId = generateEventId();

        Event e = Event.of(eventId, address, event.getClass().getName(), event);
        replyFutures.put(e.getEventId(), reply);

        doPublish(e).whenComplete((num, error) -> {
            replyFutures.remove(e.getEventId());
            if (num != null && num <= 0) {
                reply.future.complete(null);
            } else if (error != null) {
                reply.future.completeExceptionally(error);
            }
        });

        return reply.future;
    }

    @Override
    public <T> long addListener(String address, Function<T, CompletionStage<Object>> listener) {
        return addListener(address, ((Object) listener));
    }

    protected long addListener(String address, Object listener) {
        long id = getListenerId(listener);

        idListener.put(id, listener);

        getListeners(address).add(listener);

        return id;
    }

    @Override
    public <T> long addListener(String address, Consumer<T> listener) {
        return addListener(address, ((Object) listener));
    }

    @Override
    public void removeListener(String address, long id) {
        Optional.ofNullable(idListener.remove(id))
                .ifPresent(listener -> getListeners(address).remove(listener));
    }


    @Override
    public void removeListener(String address, Object listener) {
        removeListener(address, getListenerId(listener));
    }


    protected List<Object> getListeners(String address) {
        return listeners.computeIfAbsent(address, (__) -> new CopyOnWriteArrayList<>());
    }

    protected long getListenerId(Object listener) {
        return System.identityHashCode(listener);
    }
}
