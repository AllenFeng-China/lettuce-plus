package org.jetlinks.lettuce.supports;

import lombok.SneakyThrows;
import org.jetlinks.lettuce.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

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

    @Test
    @SneakyThrows
    public void testHaManager() {

        RedisHaManager redisHaManager = plus.getHaManager("test");
        redisHaManager.startup(new ServerNodeInfo("server1"));

        {
            //本机测试
            CountDownLatch latch = new CountDownLatch(1);

            redisHaManager.onNotify("test-event", String.class, data -> {
                latch.countDown();
            });

            Assert.assertTrue(redisHaManager.sendNotify("server1", "test-event", "1234")
                    .toCompletableFuture().get(10, TimeUnit.SECONDS));

            redisHaManager.onNotify("test-event-reply", String.class, (Function<String, CompletionStage<?>>) CompletableFuture::completedFuture);


            Assert.assertEquals(redisHaManager.sendNotifyReply("server1", "test-event-reply", "1234", Duration.ofSeconds(10))
                    .toCompletableFuture()
                    .get(), "1234");
        }
        {
            LettucePlus plus2 = DefaultLettucePlus.of(RedisClientHelper.createRedisClient());

            RedisHaManager redisHaManager2 = plus2.getHaManager("test");

            //测试监听节点加入
            {
                CountDownLatch joinCountDown = new CountDownLatch(1);

                redisHaManager.onNodeJoin(server -> {
                    joinCountDown.countDown();
                });

                redisHaManager2.startup(new ServerNodeInfo("server2"));

                Assert.assertTrue(joinCountDown.await(10, TimeUnit.SECONDS));

            }
            //测试发送无回复通知
            {
                CountDownLatch notifyCountDown = new CountDownLatch(1);

                redisHaManager.onNotify("test-event", String.class, data -> {
                    notifyCountDown.countDown();
                });

                Assert.assertTrue(redisHaManager2.sendNotify("server1", "test-event", "1234")
                        .toCompletableFuture().get(10, TimeUnit.SECONDS));

            }
            //测试发送回复通知
            {
                redisHaManager.onNotify("test-event-reply2", String.class, (Function<String, CompletionStage<?>>) CompletableFuture::completedFuture);

                Assert.assertEquals(redisHaManager2.sendNotifyReply("server1", "test-event-reply2", "1234", Duration.ofSeconds(10))
                        .toCompletableFuture()
                        .get(), "1234");

            }
            //测试节点下线监听器
            {
                CountDownLatch latch2 = new CountDownLatch(1);
                redisHaManager.onNodeLeave(server -> {
                    latch2.countDown();
                });

                redisHaManager2.shutdown();

                Assert.assertTrue(latch2.await(10, TimeUnit.SECONDS));
            }
        }
    }


}