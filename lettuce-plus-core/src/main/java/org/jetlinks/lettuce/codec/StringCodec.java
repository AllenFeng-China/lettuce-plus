package org.jetlinks.lettuce.codec;

import io.lettuce.core.codec.RedisCodec;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringCodec<K,V> implements RedisCodec<K, V> {

    static final StringCodec instance = new StringCodec();

    public static <K,V> StringCodec <K,V> getInstance() {
        return instance;
    }

    @Override
    public K decodeKey(ByteBuffer bytes) {
        return (K)Unpooled.wrappedBuffer(bytes).toString(StandardCharsets.UTF_8);
    }

    @Override
    public V decodeValue(ByteBuffer bytes) {
        return (V)Unpooled.wrappedBuffer(bytes).toString(StandardCharsets.UTF_8);
    }

    @Override
    public ByteBuffer encodeKey(K key) {

        return ByteBuffer.wrap(String.valueOf(key).getBytes());
    }

    @Override
    public ByteBuffer encodeValue(V value) {
        return  ByteBuffer.wrap(String.valueOf(value).getBytes());
    }
}
