package org.jetlinks.lettuce.codec;

import org.jetlinks.lettuce.supports.Event;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class FstCodecTest {


    @Test
    public void testCodec() {
        FstCodec<Object, Object> codec = new FstCodec<>();

        long time = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            Assert.assertEquals("test", codec.decodeKey(codec.encodeKey("test")));

            Assert.assertEquals("test", codec.decodeValue(codec.encodeValue("test")));


            ByteBuffer byteBuffer = codec.encodeValue(Event.of("test", "2134", "test", "1234", "aaaa"));

            Assert.assertTrue(codec.decodeValue(byteBuffer) instanceof Event);

        }
        System.out.println(System.currentTimeMillis() - time);


    }


}