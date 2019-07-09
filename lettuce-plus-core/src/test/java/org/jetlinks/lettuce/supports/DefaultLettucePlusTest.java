package org.jetlinks.lettuce.supports;

import lombok.SneakyThrows;
import org.jetlinks.lettuce.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Function;

public class DefaultLettucePlusTest {

    private LettucePlus plus;

    @Before
    public void init() {
        plus = DefaultLettucePlus.standalone(RedisClientHelper.createRedisClient());
//        plus = DefaultLettucePlus.sentinel(RedisClient.create(),
//                RedisURI.create("redis-sentinel://192.168.20.143:26380,192.168.20.143:26381,192.168.20.143:26382/0#mymaster"));
    }


    @Test
    public void testMapCache() {

        RedisLocalCacheMap<String, Object> map = plus.getLocalCacheMap("test_");
        map.clear();
        Assert.assertNull(map.get("test"));
        Assert.assertNull(map.put("test", "test"));

        long time = System.currentTimeMillis();

        for (int i1 = 0; i1 < 1000; i1++) {
            map.fastPutAsync("test", "test");
            Assert.assertEquals(map.get("test"), "test");
        }

        Assert.assertTrue(map.containsValue("test"));
        Assert.assertTrue(map.containsKey("test"));

        Assert.assertFalse(map.replace("test", "test2", "test3"));
        Assert.assertTrue(map.replace("test", "test", "test"));

        Assert.assertEquals(map.putIfAbsent("test", "test2"), "test");

        Assert.assertTrue(map.keySet().contains("test"));
        Assert.assertTrue(map.entrySet().stream().anyMatch(e -> e.getKey().equals("test")));

        Assert.assertEquals(map.remove("test"), "test");

        Assert.assertEquals(map.size(), 0);

        Assert.assertTrue(map.isEmpty());
        System.out.println(System.currentTimeMillis() - time);


    }

    @Test
    @SneakyThrows
    public void testPubSub() {
        LettucePlus plus2 = DefaultLettucePlus.standalone(RedisClientHelper.createRedisClient());

        //10 个topic 收发10000个消息
        for (int i = 0; i < 10; i++) {
            RedisTopic<String> topic = plus.getTopic("test"+i);

            CountDownLatch latch = new CountDownLatch(10000);

            topic.addListener((channel, data) -> latch.countDown());

            long time = System.currentTimeMillis();

            for (int j = 0; j < 10000; j++) {
                plus2.getTopic("test"+i).publish("test");
            }

            Assert.assertTrue(latch.await(30, TimeUnit.SECONDS));

            System.out.println(System.currentTimeMillis() - time);
        }

        CountDownLatch latch=new CountDownLatch(1);

        RedisTopic<String> topic = plus.getPatternTopic("test*");
        topic.addListener((channel, data) -> latch.countDown());

        plus2.getTopic("test123123")
                .publish("test")
                .thenAccept(System.out::println);

       Assert.assertTrue( latch.await(30,TimeUnit.SECONDS));

    }

    @Test
    @SneakyThrows
    public void testQueue() {

        for (int i = 0; i < 10; i++) {
            RedisQueue<String> queue = plus.getQueue("queue:" + i);


            CountDownLatch latch = new CountDownLatch(10000);
            queue.poll(data -> latch.countDown());

            long time = System.currentTimeMillis();

            for (int j = 0; j < 10000; j++) {
                queue.addAsync("test" + j);
            }

            latch.await(30, TimeUnit.SECONDS);
            System.out.println(System.currentTimeMillis() - time);
        }
    }

    @Test
    @SneakyThrows
    public void testHaManager() {

        RedisHaManager server1 = plus.getHaManager("test");
        server1.startup(new ServerNodeInfo("server1"));

        {
            //本机测试
            CountDownLatch latch = new CountDownLatch(1);

            server1.onNotify("test-event", String.class, data -> {
                latch.countDown();
            });

            Assert.assertTrue(server1.sendNotify("server1", "test-event", "1234")
                    .toCompletableFuture().get(10, TimeUnit.SECONDS));

            server1.onNotify("test-event-reply", String.class, (Function<String, CompletionStage<?>>) CompletableFuture::completedFuture);

            Assert.assertEquals(server1.sendNotifyReply("server1", "test-event-reply", "1234", Duration.ofSeconds(10))
                    .toCompletableFuture()
                    .get(), "1234");
        }
        {
            LettucePlus plus2 = DefaultLettucePlus.standalone(RedisClientHelper.createRedisClient());

            RedisHaManager server2 = plus2.getHaManager("test");

            //测试监听节点加入
            {
                CountDownLatch joinCountDown = new CountDownLatch(1);

                server1.onNodeJoin(server -> {
                    joinCountDown.countDown();
                });

                server2.startup(new ServerNodeInfo("server2"));

                Assert.assertTrue(joinCountDown.await(10, TimeUnit.SECONDS));

            }
            //测试发送无回复通知
            {
                CountDownLatch notifyCountDown = new CountDownLatch(1000);
                server1.onNotify("test-event", String.class, data -> {
                    notifyCountDown.countDown();
                });
                long time = System.currentTimeMillis();
                for (int i = 0; i < 1000; i++) {
                    server2.sendNotify("server1", "test-event", "1234");
                }

                Assert.assertTrue(notifyCountDown.await(30, TimeUnit.SECONDS));
                System.out.println("1k 完成通知耗时:" + (System.currentTimeMillis() - time));
            }
            //测试发送回复通知
            {
                server1.onNotify("test-event-reply2", String.class, (Function<String, CompletionStage<?>>) CompletableFuture::completedFuture);

                long time = System.currentTimeMillis();
                CountDownLatch latch = new CountDownLatch(1000);
                for (int i = 0; i < 1000; i++) {
                    server2.sendNotifyReply("server1", "test-event-reply2", "1234", Duration.ofSeconds(10))
                            .whenComplete((reply, error) -> {
                                if ("1234".equals(reply)) {
                                    latch.countDown();
                                }
                            });
                }
                latch.await(30, TimeUnit.SECONDS);
                System.out.println("1k 完成通知-回复耗时:" + (System.currentTimeMillis() - time));
            }
            //测试节点下线监听器
            {
                CountDownLatch latch2 = new CountDownLatch(1);
                server1.onNodeLeave(server -> latch2.countDown());

                server2.shutdown();

                Assert.assertTrue(latch2.await(10, TimeUnit.SECONDS));
            }
        }
    }


}