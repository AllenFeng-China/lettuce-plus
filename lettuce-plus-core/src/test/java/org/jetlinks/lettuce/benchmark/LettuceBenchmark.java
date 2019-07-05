package org.jetlinks.lettuce.benchmark;

import lombok.SneakyThrows;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.RedisClientHelper;
import org.jetlinks.lettuce.RedisQueue;
import org.jetlinks.lettuce.RedisTopic;
import org.jetlinks.lettuce.supports.DefaultLettucePlus;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class LettuceBenchmark {



    private LettucePlus plus = DefaultLettucePlus.standalone(RedisClientHelper.createRedisClient());

    private LettucePlus plus2 = DefaultLettucePlus.standalone(RedisClientHelper.createRedisClient());



    @SneakyThrows
    public static void main(String[] args) {
        Options opt = new OptionsBuilder()
                .include("LettuceBenchmark")
                .exclude("init")
                .warmupIterations(10)
                .measurementIterations(10)
                .forks(3)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @SneakyThrows
    public void testQueue() {

        int num = 1000;
        String id = UUID.randomUUID().toString();


        RedisQueue<String> queue = plus.getQueue(id);
        RedisQueue<String> queue2 = plus2.getQueue(id);

        CountDownLatch latch = new CountDownLatch(num);

        queue.poll(data -> latch.countDown());

        for (int i = 0; i < num; i++) {
            queue2.addAsync("test-"+i);
        }

        latch.await(1, TimeUnit.MINUTES);


    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @SneakyThrows
    public void testTopic() {

        int num = 1000;
        String id = UUID.randomUUID().toString();


        RedisTopic<String> queue = plus.getTopic(id);
        RedisTopic<String> queue2 = plus2.getTopic(id);

        CountDownLatch latch = new CountDownLatch(num);

        queue.addListener((topic,data) -> latch.countDown());

        for (int i = 0; i < num; i++) {
            queue2.publish("test-"+i);
        }

        latch.await(1, TimeUnit.MINUTES);
    }

}
