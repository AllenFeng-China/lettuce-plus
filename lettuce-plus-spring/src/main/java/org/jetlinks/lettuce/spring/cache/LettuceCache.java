package org.jetlinks.lettuce.spring.cache;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.lettuce.RedisLocalCacheMap;
import org.springframework.cache.support.AbstractValueAdaptingCache;

import java.util.concurrent.Callable;

@Slf4j
public class LettuceCache extends AbstractValueAdaptingCache {

    private final RedisLocalCacheMap<Object, Object> cache;

    private String name;

    protected LettuceCache(String name, RedisLocalCacheMap<Object, Object> cache, boolean allowNullValues) {
        super(allowNullValues);
        this.cache = cache;
        this.name = name;
    }

    @Override
    protected Object lookup(Object key) {
        return cache.get(key);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object getNativeCache() {
        return cache;
    }

    @Override
    @SneakyThrows
    @SuppressWarnings("all")
    public <T> T get(Object key, Callable<T> valueLoader) {

        ValueWrapper storeValue = get(key);
        if (storeValue != null) {
            return (T) storeValue.get();
        }

        synchronized (this.cache) {
            storeValue = get(key);
            if (storeValue != null) {
                return (T) storeValue.get();
            }

            T value;
            try {
                value = valueLoader.call();
            } catch (Throwable ex) {
                throw new ValueRetrievalException(key, valueLoader, ex);
            }
            put(key, value);
            return value;
        }


    }

    @Override
    public void put(Object key, Object value) {
        cache.fastPutAsync(key, value);
    }

    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        return toValueWrapper(cache.putIfAbsent(key, value));
    }

    @Override
    public void evict(Object key) {
        cache.fastRemoveAsync(key)
                .whenComplete((nil, err) -> {
                    if (err != null) {
                        log.warn("evict cache {}.{} error", getName(), key, err);
                    }
                });
    }

    @Override
    public void clear() {
        cache.clear();
    }
}
