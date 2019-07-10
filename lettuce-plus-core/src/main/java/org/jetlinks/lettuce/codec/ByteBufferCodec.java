package org.jetlinks.lettuce.codec;

import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;

public class ByteBufferCodec<K,V> implements RedisCodec<K,V> {
    @Override
    public K decodeKey(ByteBuffer bytes) {
        return (K)bytes;
    }

    @Override
    public V decodeValue(ByteBuffer bytes) {
        return (V)bytes;
    }

    @Override
    public ByteBuffer encodeKey(K key) {
        return (ByteBuffer)key;
    }

    @Override
    public ByteBuffer encodeValue(V value) {
        return (ByteBuffer)value;
    }
}
