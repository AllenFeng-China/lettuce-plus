package org.jetlinks.lettuce.spring.cache;

import io.lettuce.core.RedisClient;
import org.jetlinks.lettuce.spring.RedisClientHelper;
import org.jetlinks.lettuce.supports.DefaultLettucePlus;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.cache.Cache;

import static org.junit.Assert.*;

public class LettuceCacheManagerTest {

    LettuceCacheManager cacheManager = new LettuceCacheManager(DefaultLettucePlus.standalone(RedisClientHelper.createRedisClient()));


    @Test
    public void testCache() {
        Cache cache = cacheManager.getCache("test");
        cache.clear();

        Assert.assertNotNull(cache);

        Assert.assertNull(cache.get("test"));

        cache.put("test", "abc");

        Assert.assertEquals(cache.get("test").get(), "abc");

        Assert.assertEquals(cache.get("test2", () -> "test123"), "test123");

        Assert.assertEquals(cache.get("test2").get(), "test123");

        cache.put("test2", null);
        Assert.assertNull(cache.get("test2"));

    }
}