package org.jetlinks.lettuce.codec;

import io.lettuce.core.codec.RedisCodec;
import lombok.SneakyThrows;
import org.nustaq.serialization.*;
import org.nustaq.serialization.coders.FSTStreamDecoder;
import org.nustaq.serialization.coders.FSTStreamEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

@SuppressWarnings("all")
public class FstCodec<K, V> implements RedisCodec<K, V> {

    private FSTConfiguration config;
    private static final byte[] EMPTY = new byte[0];

    static class FSTDefaultStreamCoderFactory implements FSTConfiguration.StreamCoderFactory {

        Field chBufField;
        Field ascStringCacheField;

        {
            try {
                chBufField = FSTStreamDecoder.class.getDeclaredField("chBufS");
                ascStringCacheField = FSTStreamDecoder.class.getDeclaredField("ascStringCache");
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            ascStringCacheField.setAccessible(true);
            chBufField.setAccessible(true);
        }

        private FSTConfiguration fstConfiguration;

        FSTDefaultStreamCoderFactory(FSTConfiguration fstConfiguration) {
            this.fstConfiguration = fstConfiguration;
        }

        @Override
        public FSTEncoder createStreamEncoder() {
            return new FSTStreamEncoder(fstConfiguration);
        }

        @Override
        public FSTDecoder createStreamDecoder() {
            return new FSTStreamDecoder(fstConfiguration) {
                public String readStringUTF() throws IOException {
                    try {
                        String res = super.readStringUTF();
                        chBufField.set(this, null);
                        return res;
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                }

                @Override
                public String readStringAsc() throws IOException {
                    try {
                        String res = super.readStringAsc();
                        ascStringCacheField.set(this, null);
                        return res;
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                }
            };
        }

        static ThreadLocal input = new ThreadLocal();
        static ThreadLocal output = new ThreadLocal();

        @Override
        public ThreadLocal getInput() {
            return input;
        }

        @Override
        public ThreadLocal getOutput() {
            return output;
        }

    }

    public FstCodec(FSTConfiguration fstConfiguration) {
        config = fstConfiguration;
        config.setStreamCoderFactory(new FSTDefaultStreamCoderFactory(config));
    }

    public FstCodec() {
        this(FSTConfiguration.createDefaultConfiguration().setForceSerializable(true));
    }

    @Override
    @SneakyThrows
    public K decodeKey(ByteBuffer bytes) {
        FSTObjectInput input = config.getObjectInput(new ByteBufferBackedInputStream(bytes));
        return (K) input.readObject();
    }

    @Override
    @SneakyThrows
    public V decodeValue(ByteBuffer bytes) {

        return (V) config.getObjectInput(new ByteBufferBackedInputStream(bytes))
                .readObject();
    }

    @Override
    @SneakyThrows
    public ByteBuffer encodeKey(K key) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        FSTObjectOutput output = config.getObjectOutput(stream);
        output.writeObject(key);
        output.flush();

        return ByteBuffer.wrap(stream.toByteArray());
    }

    @Override
    @SneakyThrows
    public ByteBuffer encodeValue(V value) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        FSTObjectOutput output = config.getObjectOutput(stream);
        output.writeObject(value);
        output.flush();
        return ByteBuffer.wrap(stream.toByteArray());
    }
}