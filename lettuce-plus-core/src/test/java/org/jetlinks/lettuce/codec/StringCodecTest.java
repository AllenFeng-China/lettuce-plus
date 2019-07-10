package org.jetlinks.lettuce.codec;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class StringCodecTest {

    @Test
    public void test(){

        StringCodec<String,String> codec=StringCodec.getInstance();

        Assert.assertEquals(codec.decodeKey(codec.encodeKey("123")),"123");

        Assert.assertEquals(codec.decodeValue(codec.encodeValue("123")),"123");

    }
}