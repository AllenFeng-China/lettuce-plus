package org.jetlinks.lettuce.spring.cache;

import org.jetlinks.lettuce.LettucePlus;
import org.springframework.cache.Cache;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;

public class LettuceCacheManager extends ConcurrentMapCacheManager {

    private LettucePlus plus;

    public LettuceCacheManager(LettucePlus plus) {
        this.plus = plus;
        setAllowNullValues(true);
    }

    @Override
    protected Cache createConcurrentMapCache(String name) {

        return new LettuceCache(name,
                plus.getLocalCacheMap(name),
                isAllowNullValues());
    }
}
