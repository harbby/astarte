/*
 * Copyright (C) 2018 The Astarte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.harbby.astarte.core.coders.io;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;

public class LZ4WritableByteChannelTest
{
    @Test
    public void checkTestWhenInputIs1x10()
            throws IOException
    {
        byte[] msg = "11111111111111".getBytes(StandardCharsets.UTF_8);
        Assert.assertArrayEquals(lz4EncoderByNioChannel(msg), lz4EncoderByOutputStream(msg));
    }

    @Test
    public void checkTestWhenInputIsHelloWorld()
            throws IOException
    {
        byte[] msg = "hello world!".getBytes(StandardCharsets.UTF_8);
        Assert.assertArrayEquals(lz4EncoderByNioChannel(msg), lz4EncoderByOutputStream(msg));
    }

    @Test
    public void checkBigMsg()
            throws IOException
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            builder.append("hello world!").append(i);
        }
        byte[] bytes = builder.toString().getBytes(StandardCharsets.UTF_8);
        Assert.assertArrayEquals(lz4EncoderByNioChannel(bytes), lz4EncoderByOutputStream(bytes));
    }

    private static byte[] lz4EncoderByNioChannel(byte[] bytes)
            throws IOException
    {
        try (MemoryWritableByteChannel memoryChannelBuffer = new MemoryWritableByteChannel();
                LZ4WritableByteChannel channel = new LZ4WritableByteChannel(memoryChannelBuffer);) {
            channel.write(ByteBuffer.wrap(bytes));
            channel.finish();
            return memoryChannelBuffer.toByteArray();
        }
    }

    private static byte[] lz4EncoderByOutputStream(byte[] bytes)
            throws IOException
    {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                LZ4BlockOutputStream lz4BlockOutputStream = new LZ4BlockOutputStream(byteArrayOutputStream, Checksums.lengthCheckSum())) {
            lz4BlockOutputStream.write(bytes);
            lz4BlockOutputStream.finishBlock();
            return byteArrayOutputStream.toByteArray();
        }
    }

    private static class MemoryWritableByteChannel
            implements WritableByteChannel
    {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        @Override
        public int write(ByteBuffer src)
                throws IOException
        {
            int i = 0;
            int remaining = src.remaining();
            for (; i < remaining; i++) {
                buffer.write(src.get());
            }
            return remaining;
        }

        @Override
        public boolean isOpen()
        {
            return true;
        }

        @Override
        public void close()
                throws IOException
        {
            buffer.close();
        }

        public byte[] toByteArray()
        {
            return buffer.toByteArray();
        }
    }
}
