package org.jetlinks.lettuce.codec;

import io.lettuce.core.codec.RedisCodec;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringKeyCodec<V> implements RedisCodec<String, V> {

    private RedisCodec<Object, V> valueCodec;

    public StringKeyCodec(RedisCodec<Object, V> valueCodec){
        this.valueCodec=valueCodec;
    }

    @Override
    public String decodeKey(ByteBuffer bytes) {
        return Unpooled.wrappedBuffer(bytes).toString(StandardCharsets.UTF_8);
    }

    @Override
    public V decodeValue(ByteBuffer bytes) {
        return valueCodec.decodeValue(bytes);
    }

    @Override
    public ByteBuffer encodeKey(String key) {

        return ByteBuffer.wrap(key.getBytes());
    }

    @Override
    public ByteBuffer encodeValue(V value) {
        return valueCodec.encodeValue(value);
    }
}
