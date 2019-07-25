package org.jetlinks.lettuce;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * redis queue
 *
 * @param <T>
 * @since 1.0
 */
public interface RedisQueue<T> {

    /**
     * 设置本地消费概率,当本地设置了消费者时,可随机使用本地消费者进行消费
     *
     * @param point 概率, 0到1之间到浮点数
     */
    void setLocalConsumerPoint(float point);

    /**
     * 持续消费数据,当队列中有数据传入后,将触发监听器
     *
     * @param listener 数据监听器
     */
    void poll(Consumer<T> listener);

    /**
     * 消费一条数据,如果队列中无数据,则{@link CompletableFuture#get()}为null
     *
     * @return 消费结果
     * @see CompletableFuture
     * @see CompletionStage#toCompletableFuture()
     */
    CompletionStage<T> poll();

    /**
     * 移除监听器
     *
     * @param listener 监听器实例
     * @see this#poll(Consumer)
     */
    void removeListener(Consumer<T> listener);

    /**
     * 添加多个数据到队列
     *
     * @param data 数据集合
     * @return 添加结果
     */
    CompletionStage<Boolean> addAll(Collection<T> data);

    /**
     * 添加一个数据到队列
     *
     * @param data 数据
     * @return 添加结果
     */
    CompletionStage<Boolean> addAsync(T data);

    /**
     * 清空队列数据以及监听者,重置整个队列
     *
     * @return 处理结果
     */
    CompletionStage<Void> clear();

}
