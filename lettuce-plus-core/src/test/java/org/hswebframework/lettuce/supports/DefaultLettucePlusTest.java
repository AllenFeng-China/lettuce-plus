package org.hswebframework.lettuce.supports;

import lombok.SneakyThrows;
import org.hswebframework.lettuce.LettucePlus;
import org.hswebframework.lettuce.RedisClientHelper;
import org.hswebframework.lettuce.RedisQueue;
import org.hswebframework.lettuce.RedisTopic;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultLettucePlusTest {

    private LettucePlus plus;

    @Before
    public void init() {
        plus = DefaultLettucePlus.of(RedisClientHelper.createRedisClient());
    }

    @Test
    @SneakyThrows
    public void testPubSub() {
        RedisTopic<String> topic = plus.getTopic("test");

        CountDownLatch latch = new CountDownLatch(10000);

        topic.addListener((channel, data) -> latch.countDown());

        long time = System.currentTimeMillis();

        for (int i = 0; i < 10000; i++) {
            topic.publish("test");
        }

        Assert.assertTrue(latch.await(30, TimeUnit.SECONDS));

        System.out.println(System.currentTimeMillis() - time);
    }

    @Test
    @SneakyThrows
    public void testQueue() {

        RedisQueue<String> queue = plus.getQueue(UUID.randomUUID().toString());

        Assert.assertTrue(queue.addAsync("test").toCompletableFuture().get(10, TimeUnit.SECONDS));

        Assert.assertEquals(queue.poll().toCompletableFuture().get(10, TimeUnit.SECONDS), "test");

        CountDownLatch latch = new CountDownLatch(10000);

        AtomicLong counter = new AtomicLong();
        long time = System.currentTimeMillis();

        queue.poll(data -> {
            counter.incrementAndGet();
//            System.out.println(counter);
            latch.countDown();
        });
        System.out.println("1234");
        for (int i = 0; i < 10000; i++) {
            queue.addAsync("test" + i);
        }

        System.out.println(System.currentTimeMillis() - time);

        Assert.assertTrue(latch.await(30, TimeUnit.SECONDS));

        System.out.println(System.currentTimeMillis() - time);
    }

}