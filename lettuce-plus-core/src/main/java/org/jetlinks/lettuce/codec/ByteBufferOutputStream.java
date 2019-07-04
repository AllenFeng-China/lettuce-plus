package org.jetlinks.lettuce.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferOutputStream extends ByteArrayOutputStream {


    public ByteBuffer getBuffer() {
        return ByteBuffer.wrap(this.toByteArray());
    }
}