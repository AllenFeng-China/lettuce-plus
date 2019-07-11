package org.jetlinks.lettuce.spring.cache;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.lettuce.LettucePlus;
import org.springframework.cache.Cache;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;

public class LettuceCacheManager extends ConcurrentMapCacheManager {

    private LettucePlus plus;

    @Setter
    @Getter
    private String cachePrefix = "spring-cache:";

    public LettuceCacheManager(LettucePlus plus) {
        this.plus = plus;
        setAllowNullValues(true);
    }

    @Override
    protected Cache createConcurrentMapCache(String name) {

        return new LettuceCache(name,
                plus.getLocalCacheMap(cachePrefix.concat(name)),
                isAllowNullValues());
    }
}
