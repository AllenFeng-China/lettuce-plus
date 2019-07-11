package org.jetlinks.lettuce.supports;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.masterslave.MasterSlave;
import io.lettuce.core.masterslave.StatefulRedisMasterSlaveConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.sentinel.api.sync.RedisSentinelCommands;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.RedisTopic;
import org.jetlinks.lettuce.codec.ByteBufferCodec;
import org.jetlinks.lettuce.codec.FstCodec;
import org.jetlinks.lettuce.codec.StringKeyCodec;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.*;

@SuppressWarnings("all")
@Slf4j
public class DefaultLettucePlus extends AbstractLettucePlus {

    @Setter
    @Getter
    private int poolSize = Runtime.getRuntime().availableProcessors() * 2;

    @Setter
    @Getter
    private int pubsubSize = Runtime.getRuntime().availableProcessors();

    private Random random = new Random();

    private RedisClient redisClient;

    private List<StatefulRedisConnection<?, ?>> connections = new CopyOnWriteArrayList<>();

    private List<StatefulRedisPubSubConnection<String, Object>> pubSubConnections = new CopyOnWriteArrayList<>();

    public DefaultLettucePlus(RedisClient client) {
        this.redisClient = client;
    }

    public static LettucePlus standalone(RedisClient client) {
        DefaultLettucePlus plus = new DefaultLettucePlus(client);
        plus.setExecutorService(Executors.newScheduledThreadPool(16));
        plus.setDefaultCodec(new StringKeyCodec<>(new FstCodec<>()));
        plus.init();
        plus.initStandalone();
        return plus;
    }

    public static LettucePlus sentinel(RedisClient client, RedisURI redisURI) {

        DefaultLettucePlus plus = new DefaultLettucePlus(client);
        plus.setExecutorService(Executors.newScheduledThreadPool(16));
        plus.setDefaultCodec(new StringKeyCodec<>(new FstCodec<>()));
        plus.init();
        plus.initSentinel(redisURI);

        return plus;
    }

    public void initSentinel(RedisURI redisURI) {
        log.info("init redis sentinel connection pool : {}", poolSize);
        {
            RedisSentinelCommands<String, String> connection = redisClient.connectSentinel(redisURI).sync();
            Map<String, String> master = connection.master("mymaster");
        }
        for (int i = 0; i < poolSize; i++) {
            StatefulRedisMasterSlaveConnection<?, ?> connection = MasterSlave.connect(redisClient, getDefaultCodec(), redisURI);
            connection.setReadFrom(ReadFrom.SLAVE_PREFERRED);
            connection.setAutoFlushCommands(true);
            connections.add(connection);
        }

        RedisClient client = RedisClient.create(redisClient.getResources(), redisURI);
        log.info("init redis pubsub connection pool : {}", pubsubSize);
        for (int i = 0; i < pubsubSize; i++) {
            StatefulRedisPubSubConnection<String, Object> connection = client.connectPubSub(new StringKeyCodec<>(new ByteBufferCodec<>()));
            connection.setAutoFlushCommands(true);
            bindPubSubListener(connection);
            pubSubConnections.add(connection);
        }
    }

    public void initStandalone() {
        log.info("init redis connection pool : {}", poolSize);
        for (int i = 0; i < poolSize; i++) {
            StatefulRedisConnection<?, ?> connection = redisClient.connect(getDefaultCodec());
            connection.setAutoFlushCommands(true);
            connections.add(connection);
        }

        log.info("init redis pubsub connection pool : {}", pubsubSize);
        for (int i = 0; i < pubsubSize; i++) {
            StatefulRedisPubSubConnection<String, Object> connection = redisClient.connectPubSub(new StringKeyCodec<>(new ByteBufferCodec<>()));
            connection.setAutoFlushCommands(true);
            bindPubSubListener(connection);
            pubSubConnections.add(connection);
        }
    }

    public void init() {
        super.init();
    }

    @Override
    public <K, V> CompletionStage<StatefulRedisConnection<K, V>> getConnection() {
        return CompletableFuture.completedFuture((StatefulRedisConnection) connections.get(random.nextInt(connections.size())));
    }

    protected <K, V> CompletionStage<StatefulRedisPubSubConnection<K, V>> getPubSubConnect() {
        return CompletableFuture.completedFuture((StatefulRedisPubSubConnection) pubSubConnections.get(random.nextInt(pubSubConnections.size())));
    }

    private Map<String, StatefulRedisPubSubConnection<String, ?>> topicConnections = new ConcurrentHashMap<>();

    @Override
    protected void subscripe(String topic, boolean pattern) {

        topicConnections.computeIfAbsent(topic, _topic -> {
            try {
                StatefulRedisPubSubConnection connection = getPubSubConnect().toCompletableFuture().get(10, TimeUnit.SECONDS);
                if (pattern) {
                    connection.sync().psubscribe(_topic);
                } else {
                    connection.sync().subscribe(_topic);
                }
                return connection;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        });
    }


    @Override
    protected void unsubscripe(String topic) {
        Optional.ofNullable(topicConnections.remove(topic))
                .ifPresent(conn -> {
                    RedisTopic<?> redisTopic = redisTopicMap.get(topic);
                    if (redisTopic == null) {
                        conn.sync().unsubscribe(topic);
                        conn.sync().punsubscribe(topic);
                    } else {
                        if (redisTopic.isPattern()) {
                            conn.sync().punsubscribe(topic);
                        } else {
                            conn.sync().unsubscribe(topic);
                        }
                    }
                });

    }
}
