package org.jetlinks.lettuce.supports;

import io.lettuce.core.KeyValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.output.BooleanOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.RedisLocalCacheMap;
import org.jetlinks.lettuce.RedisTopic;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings("all")
public class DefaultRedisLocalCacheMap<K, V> implements RedisLocalCacheMap<K, V> {

    private LettucePlus plus;

    private Map<Object, Reference<Cache>> cache = new ConcurrentHashMap<>(128);

    private String redisKey;

    private RedisTopic<Object> clearTopic;

    public DefaultRedisLocalCacheMap(String id, LettucePlus plus) {
        this.redisKey = id;
        this.plus = plus;

        clearTopic = plus.getTopic("_local:cache:changed:".concat(id));
        clearTopic.addListener((channel, key) -> {
            if ("__all".equals(key)) {
                cache.clear();
            } else {
                cache.remove(key);
            }
        });
    }

    @Getter
    @Setter
    private class Cache {
        private V value;

        private long leaveTime;

        @Override
        public int hashCode() {
            return value == null ? 0 : value.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return value != null && value.equals(obj);
        }
    }

    @SneakyThrows
    private <K, V> StatefulRedisConnection<K, V> getSyncRedis() {
        return plus.<K, V>getConnection()
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
    }

    protected <K, V> CompletionStage<RedisAsyncCommands<K, V>> getAsyncRedis() {
        return plus
                .<K, V>getConnection()
                .thenApply(StatefulRedisConnection::async);
    }

    @Override
    public int size() {
        return getSyncRedis()
                .sync()
                .hlen(redisKey)
                .intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return getSyncRedis().sync().hexists(redisKey, key);
    }

    @Override
    @SneakyThrows
    public boolean containsValue(Object value) {
        String script = "local s = redis.call('hvals', KEYS[1]);" +
                "for i = 1, #s, 1 do " +
                "if ARGV[1] == s[i] then " +
                "return 1 " +
                "end " +
                "end;" +
                "return 0";
        AsyncCommand<Object, Object, Boolean> command = new AsyncCommand<>(new Command<>(ValueChangCommand.EVAL,
                new BooleanOutput<>(plus.getDefaultCodec()),
                new CommandArgs<>(plus.getDefaultCodec())
                        .add(script)
                        .add(1)
                        .addKeys(redisKey)
                        .addValues(value)));

        this.getSyncRedis().dispatch(command);

        return command.get();
    }

    private Reference<Cache> wrapCache(Object value) {
        Cache cache = new Cache();
        cache.value = (V) value;
        return new SoftReference<>(cache);
    }

    @Override
    public V get(Object key) {

        Reference<Cache> reference = cache.get(key);

        if (reference == null || reference.get() == null) {
            cache.put(key, reference = wrapCache(this.getSyncRedis().sync().hget(redisKey, key)));
        }

        Cache localCache = reference.get();
        if (localCache == null) {
            return null;
        }
        return localCache.value;

    }

    @SneakyThrows
    public void fastPut(K key, V value) {
        fastPutAsync(key, value);//.toCompletableFuture().get();
    }


    public CompletionStage<Void> fastPutAsync(K key, V value) {
        String script = "" +
                "redis.call('hset',KEYS[1],KEYS[2],ARGV[1]);" +
                "redis.call('publish',KEYS[3],ARGV[2]);" +
                "return nil;";

        AsyncCommand<Object, V, V> command = new AsyncCommand<>(new Command<>(ValueChangCommand.EVAL,
                new ValueOutput<>(plus.getDefaultCodec()),
                new CommandArgs<>(plus.<Object, V>getDefaultCodec())
                        .add(script)
                        .add(3)
                        .addKeys(redisKey, key, "_local:cache:changed:".concat(redisKey))
                        .addValues(value, (V) key)));

        this.<Object, V>getSyncRedis().dispatch(command);
        cache.put(key, wrapCache(value));

        return command
                .thenApply((nil) -> null);
    }

    public CompletionStage<V> putAsync(K key, V value) {
        String script = "" +
                "local val = redis.call('hget',KEYS[1],KEYS[2]);" +
                "redis.call('hset',KEYS[1],KEYS[2],ARGV[1]);" +
                "redis.call('publish',KEYS[3],ARGV[2]);" +
                "return val;";

        AsyncCommand<Object, V, V> command = new AsyncCommand<>(new Command<>(ValueChangCommand.EVAL,
                new ValueOutput<>(plus.getDefaultCodec()),
                new CommandArgs<>(plus.<Object, V>getDefaultCodec())
                        .add(script)
                        .add(3)
                        .addKeys(redisKey, key, "_local:cache:changed:".concat(redisKey))
                        .addValues(value, (V) key)));

        this.<Object, V>getSyncRedis().dispatch(command);

        cache.put(key, wrapCache(value));

        return command;
    }

    @Override
    @SneakyThrows
    public V put(K key, V value) {
        return putAsync(key, value).toCompletableFuture().get();
    }

    public CompletionStage<V> removeAsync(Object key) {

        String script = "" +
                "local val = redis.call('hget',KEYS[1],KEYS[2]);" +
                "redis.call('hdel',KEYS[1],KEYS[2]);" +
                "redis.call('publish',KEYS[3],ARGV[1]);" +
                "return val;";
        AsyncCommand<Object, V, V> command = new AsyncCommand<>(new Command<>(ValueChangCommand.EVAL,
                new ValueOutput<>(plus.getDefaultCodec()),
                new CommandArgs<>(plus.<Object, V>getDefaultCodec())
                        .add(script)
                        .add(3)
                        .addKeys(redisKey, key, "_local:cache:changed:".concat(redisKey))
                        .addValue((V) key)));

        this.<Object, V>getSyncRedis().dispatch(command);

        cache.remove(key);

        return command;
    }

    @Override
    @SneakyThrows
    public V remove(Object key) {
        return removeAsync(key).toCompletableFuture().get();
    }

    @Override
    public CompletionStage<Void> fastRemoveAllAsync(K... key) {
        return this.<K, V>getSyncRedis()
                .async()
                .hdel((K) redisKey, key)
                .thenApply(n -> null);
    }

    public CompletionStage<Void> fastRemoveAsync(K key) {

        String script = "" +
                "redis.call('hdel',KEYS[1],KEYS[2]);" +
                "redis.call('publish',KEYS[3],ARGV[1]);" +
                "return nil;";

        AsyncCommand<Object, V, V> command = new AsyncCommand<>(new Command<>(ValueChangCommand.EVAL,
                new ValueOutput<>(plus.getDefaultCodec()),
                new CommandArgs<>(plus.<Object, V>getDefaultCodec())
                        .add(script)
                        .add(3)
                        .addKeys(redisKey, key, "_local:cache:changed:".concat(redisKey))
                        .addValue((V) key)));

        this.<Object, V>getSyncRedis().dispatch(command);

        cache.remove(key);

        return command.thenApply(nil -> null);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        this.getSyncRedis()
                .sync()
                .hmset(redisKey, (Map) m);

        m.keySet().forEach(this.cache::remove);
    }

    @Override
    public void clear() {
        cache.clear();
        this.getSyncRedis().sync()
                .del(redisKey);
        clearTopic.publish("__all");
    }

    @Override
    public Set<K> keySet() {

        return this.<K, V>getSyncRedis()
                .sync()
                .hkeys((K) redisKey)
                .stream()
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<V> values() {

        return this.getSyncRedis()
                .sync()
                .hvals(redisKey)
                .stream()
                .map(val -> ((V) val))
                .collect(Collectors.toList());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {


        return this.<K, V>getSyncRedis()
                .sync()
                .hgetall((K) redisKey)
                .entrySet();
    }

    @AllArgsConstructor
    class KeyValueEntry implements Entry<K, V> {

        private KeyValue<K, V> kv;

        private V val;

        @Override
        public K getKey() {
            return kv.getKey();
        }

        @Override
        public V getValue() {
            return val == null ? val = kv.getValueOrElse(null) : val;
        }

        @Override
        public V setValue(V value) {
            V old = val;
            val = value;
            return old;
        }
    }

    @Override
    @SneakyThrows
    public V putIfAbsent(K key, V value) {

        String script = "if redis.call('hsetnx', KEYS[1], KEYS[2], ARGV[1]) == 1 then "
                + "return nil "
                + "else "
                + "redis.call('publish',KEYS[3],ARGV[2]); "
                + "return redis.call('hget', KEYS[1], KEYS[2]); "
                + "end";

        AsyncCommand<Object, V, V> command = new AsyncCommand<>(new Command<>(ValueChangCommand.EVAL,
                new ValueOutput<>(plus.getDefaultCodec()),
                new CommandArgs<>(plus.<Object, V>getDefaultCodec())
                        .add(script)
                        .add(3)
                        .addKeys(redisKey, key, "_local:cache:changed:".concat(redisKey))
                        .addValues(value,(V)key)));

        this.<Object, V>getSyncRedis().dispatch(command);

        return command.get();
    }

    @Override
    @SneakyThrows
    public boolean remove(Object key, Object value) {
        String script = "if redis.call('hget', KEYS[1], KEYS[2]) == ARGV[1] then "
                + "redis.call('publish',KEYS[3],KEYS[2]); "
                + "return redis.call('hdel', KEYS[1], ARGV[2]) "
                + "else "
                + "return 0 "
                + "end";

        AsyncCommand<Object, Object, Boolean> command = new AsyncCommand<>(new Command<>(ValueChangCommand.EVAL,
                new BooleanOutput<>(plus.getDefaultCodec()),
                new CommandArgs<>(plus.getDefaultCodec())
                        .add(script)
                        .add(3)
                        .addKeys(redisKey, key, "_local:cache:changed:".concat(redisKey))
                        .addValues(value,key)));

        this.getSyncRedis().dispatch(command);

        return command.get();
    }

    @Override
    @SneakyThrows
    public boolean replace(K key, V oldValue, V newValue) {
        String script = "if redis.call('hget', KEYS[1], KEYS[2]) == ARGV[1] then "
                + "redis.call('hset', KEYS[1], KEYS[2], ARGV[2]); "
                + "redis.call('publish',KEYS[3],ARGV[3]); "
                + "return 1; "
                + "else "
                + "return 0; "
                + "end";

        AsyncCommand<Object, Object, Boolean> command = new AsyncCommand<>(new Command<>(ValueChangCommand.EVAL,
                new BooleanOutput<>(plus.getDefaultCodec()),
                new CommandArgs<>(plus.getDefaultCodec())
                        .add(script)
                        .add(3)
                        .addKeys(redisKey, key, "_local:cache:changed:".concat(redisKey))
                        .addValues(oldValue, newValue,key)));

        this.getSyncRedis().dispatch(command);

        return command.get();
    }

    @Override
    @SneakyThrows
    @SuppressWarnings("all")
    public V replace(K key, V value) {

        String script = "if redis.call('hexists', KEYS[1], KEYS[2]) == 1 then "
                + "local v = redis.call('hget', KEYS[1], KEYS[2]); "
                + "redis.call('hset', KEYS[1], KEYS[2], ARGV[1]); "
                + "redis.call('publish',KEYS[3],ARGV[2]); "
                + "return v; "
                + "else "
                + "return nil; "
                + "end";

        AsyncCommand<Object, V, V> command = new AsyncCommand<>(new Command<>(ValueChangCommand.EVAL,
                new ValueOutput<>(plus.getDefaultCodec()),
                new CommandArgs<>(plus.<Object, V>getDefaultCodec())
                        .add(script)
                        .add(3)
                        .addKeys(redisKey, key, "_local:cache:changed:".concat(redisKey))
                        .addValues(value,(V)key)));

        this.<Object, V>getSyncRedis().dispatch(command);
        return command.get();
    }

    enum ValueChangCommand implements ProtocolKeyword {
        EVAL;

        private final byte[] name = name().getBytes();

        @Override
        public byte[] getBytes() {
            return name;
        }

    }
}
