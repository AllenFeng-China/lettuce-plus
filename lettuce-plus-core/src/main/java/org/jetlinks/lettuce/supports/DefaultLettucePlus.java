package org.jetlinks.lettuce.supports;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.codec.FstCodec;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

@SuppressWarnings("all")
public class DefaultLettucePlus extends AbstractLettucePlus {

    @Setter
    @Getter
    private int poolSize = Runtime.getRuntime().availableProcessors() * 2;

    @Setter
    @Getter
    private int pubsubSize = Runtime.getRuntime().availableProcessors();

    private RedisClient redisClient;

    private List<StatefulRedisConnection<?, ?>> connections = new CopyOnWriteArrayList<>();

    private List<StatefulRedisPubSubConnection<String, Object> > pubSubConnections = new CopyOnWriteArrayList<>();

    public DefaultLettucePlus(RedisClient client) {
        this.redisClient = client;
    }

    public static LettucePlus of(RedisClient client) {
        DefaultLettucePlus plus = new DefaultLettucePlus(client);
        plus.setExecutorService(Executors.newScheduledThreadPool(16));
        plus.setDefaultCodec(new FstCodec<>());
        plus.init();

        return plus;
    }

    public void init() {
        for (int i = 0; i < poolSize; i++) {
            StatefulRedisConnection<?, ?> connection = redisClient.connect(getDefaultCodec());
            connection.setAutoFlushCommands(true);
            connections.add(connection);
        }
        for (int i = 0; i < pubsubSize; i++) {
            StatefulRedisPubSubConnection<String, Object> connection= redisClient.connectPubSub((RedisCodec)getDefaultCodec());
            connection.setAutoFlushCommands(true);
            bindPubSubListener(connection);
            pubSubConnections.add(connection);
        }

    }

    private Random random = new Random();

    @Override
    public <K, V> CompletionStage<StatefulRedisConnection<K, V>> getConnection(RedisCodec<K, V> codec, Duration timeout) {

        return CompletableFuture.completedFuture((StatefulRedisConnection) connections.get(random.nextInt(connections.size())));
    }

    @Override
    protected <K, V> CompletionStage<StatefulRedisPubSubConnection<K, V>> getPubSubConnect() {
        return CompletableFuture.completedFuture((StatefulRedisPubSubConnection) pubSubConnections.get(random.nextInt(pubSubConnections.size())));
    }

    @Override
    protected void subscripe(String topic) {
        for (StatefulRedisPubSubConnection<String, Object>  pubSubConnection : pubSubConnections) {
            pubSubConnection.sync().subscribe(topic);
        }
    }

    @Override
    protected void unsubscripe(String topic) {
        for (StatefulRedisPubSubConnection<String, Object>  pubSubConnection : pubSubConnections) {
            pubSubConnection.sync().unsubscribe(topic);
        }
    }
}
