package org.jetlinks.lettuce.codec;

import org.jetlinks.lettuce.supports.Notify;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;

public class FstCodecTest {


    @Test
    public void testCodec() {
        FstCodec<Object, Object> codec = new FstCodec<>();

        long time = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            Assert.assertEquals("test", codec.decodeKey(codec.encodeKey("test")));

            Assert.assertEquals("test", codec.decodeValue(codec.encodeValue("test")));


            ByteBuffer byteBuffer = codec.encodeValue(Notify.of("test", "2134", "test", "1234", "aaaa"));

            Assert.assertTrue(codec.decodeValue(byteBuffer) instanceof Notify);

        }
        System.out.println(System.currentTimeMillis() - time);


    }


    @Test
    public void testCodecObject(){
        FstCodec<Object, Object> codec = new FstCodec<>();
        long time = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            TestEntity entity=new TestEntity();
            entity.setId("123456");
            entity.setData(new HashMap<>());
            entity.setNum(i);
            entity.setNest(new HashMap<>(Collections.singletonMap("test","test")));

            Assert.assertEquals(entity, codec.decodeValue(codec.encodeValue(entity)));
        }
        System.out.println(System.currentTimeMillis() - time);


    }


}