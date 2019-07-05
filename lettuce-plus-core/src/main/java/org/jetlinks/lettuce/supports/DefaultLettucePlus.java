package org.jetlinks.lettuce.supports;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.masterslave.MasterSlave;
import io.lettuce.core.masterslave.StatefulRedisMasterSlaveConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;
import io.lettuce.core.sentinel.api.sync.RedisSentinelCommands;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.codec.FstCodec;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        plus.setDefaultCodec(new FstCodec<>());
        plus.init();
        plus.initStandalone();
        return plus;
    }

    public static LettucePlus sentinel(RedisClient client, RedisURI redisURI) {

        DefaultLettucePlus plus = new DefaultLettucePlus(client);
        plus.setExecutorService(Executors.newScheduledThreadPool(16));
        plus.setDefaultCodec(new FstCodec<>());
        plus.init();
        plus.initSentinel(redisURI);

        return plus;
    }

    private void initSentinel(RedisURI redisURI) {
        log.info("init redis sentinel connection pool : {}", poolSize);
        {
            RedisSentinelCommands<String, String> connection = redisClient.connectSentinel(redisURI).sync();
            Map<String, String> master = connection.master("mymaster");
            System.out.println(master);
            System.out.println(connection.slaves("mymaster"));
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
            StatefulRedisPubSubConnection<String, Object> connection = client.connectPubSub((RedisCodec) getDefaultCodec());
            connection.setAutoFlushCommands(true);
            bindPubSubListener(connection);
            pubSubConnections.add(connection);
        }
    }

    private void initStandalone() {
        log.info("init redis connection pool : {}", poolSize);
        for (int i = 0; i < poolSize; i++) {
            StatefulRedisConnection<?, ?> connection = redisClient.connect(getDefaultCodec());
            connection.setAutoFlushCommands(true);
            connections.add(connection);
        }

        log.info("init redis pubsub connection pool : {}", pubsubSize);
        for (int i = 0; i < pubsubSize; i++) {
            StatefulRedisPubSubConnection<String, Object> connection = redisClient.connectPubSub((RedisCodec) getDefaultCodec());
            connection.setAutoFlushCommands(true);
            bindPubSubListener(connection);
            pubSubConnections.add(connection);
        }
    }

    protected void init() {
        super.init();
    }

    @Override
    public <K, V> CompletionStage<StatefulRedisConnection<K, V>> getConnection(RedisCodec<K, V> codec, Duration timeout) {
        CompletableFuture<StatefulRedisConnection<K, V>> future = new CompletableFuture<>();

        if (connections.isEmpty()) {
            future.completeExceptionally(new IllegalStateException("connnection closed"));
            return future;
        }
        StatefulRedisConnection<K, V> connection = (StatefulRedisConnection) connections.get(random.nextInt(connections.size()));
        if (connection.isOpen()) {
            future.complete(connection);
        } else {
            connections.remove(connection);
            return getConnection(codec, timeout);
        }

        return future;
    }

    @Override
    protected <K, V> CompletionStage<StatefulRedisPubSubConnection<K, V>> getPubSubConnect() {
        return CompletableFuture.completedFuture((StatefulRedisPubSubConnection) pubSubConnections.get(random.nextInt(pubSubConnections.size())));
    }

    @Override
    protected void subscripe(String topic) {
        for (StatefulRedisPubSubConnection<String, Object> pubSubConnection : pubSubConnections) {
            pubSubConnection.async().subscribe(topic);
        }
    }

    @Override
    protected void unsubscripe(String topic) {
        for (StatefulRedisPubSubConnection<String, Object> pubSubConnection : pubSubConnections) {
            pubSubConnection.async().unsubscribe(topic);
        }
    }
}
