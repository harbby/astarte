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

import com.github.harbby.gadtry.base.Platform;
import com.github.harbby.gadtry.base.Throwables;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public final class UnsafeOffHeapDataOutput
        extends OutputStream
        implements DataOutputView
{
    private static final Unsafe unsafe = Platform.getUnsafe();
    private final int blockSize;
    private final long address;
    private int index = 0;
    private final WritableByteChannel channel;
    private final ByteBuffer byteBuffer;

    public UnsafeOffHeapDataOutput(WritableByteChannel channel)
    {
        this(channel, 1 << 16);
    }

    public UnsafeOffHeapDataOutput(WritableByteChannel channel, int buffSize)
    {
        this.channel = channel;
        this.byteBuffer = Platform.allocateDirectBuffer(buffSize);
        this.address = ((DirectBuffer) byteBuffer).address();
        this.blockSize = byteBuffer.capacity();
    }

    private void checkSize(int size)
    {
        if (index + size >= blockSize) {
            this.flush();
        }
    }

    @Override
    public void write(byte[] b)
    {
        this.write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len)
    {
        int ramming = blockSize - index;
        while (len > ramming) {
            unsafe.copyMemory(b, off, address, index, ramming);
            index = blockSize;
            this.flush();
            off += ramming;
            len -= ramming;
            ramming = blockSize;
        }
        unsafe.copyMemory(b, off, address, index, len);
        index += len;
    }

    @Override
    public void close()
    {
        try (WritableByteChannel ignored = this.channel) {
            this.flush();
        }
        catch (IOException e) {
            Throwables.throwThrowable(e);
        }
    }

    @Override
    public void flush()
    {
        byteBuffer.position(0);
        byteBuffer.limit(index);
        try {
            channel.write(byteBuffer);
        }
        catch (IOException e) {
            Throwables.throwThrowable(e);
        }
        byteBuffer.clear();
        this.index = 0;
    }

    @Override
    public void write(int b)
    {
        checkSize(1);
        unsafe.putByte(null, address + index, (byte) b);
        index++;
    }

    @Override
    public void writeBoolean(boolean v)
    {
        checkSize(1);
        unsafe.putBoolean(null, address + index, v);
        index++;
    }

    @Override
    public void writeByte(int v)
    {
        this.write(v);
    }

    @Override
    public void writeShort(int v)
    {
        checkSize(2);
        unsafe.putShort(null, address + index, (short) v);
        index += 2;
    }

    @Override
    public void writeChar(int v)
    {
        checkSize(2);
        unsafe.putChar(null, address + index, (char) v);
        index += 2;
    }

    @Override
    public void writeInt(int v)
    {
        checkSize(4);
        unsafe.putInt(null, address + index, v);
        index += 4;
    }

    @Override
    public void writeLong(long v)
    {
        checkSize(8);
        unsafe.putLong(null, address + index, v);
        index += 8;
    }

    @Override
    public void writeFloat(float v)
    {
        checkSize(4);
        unsafe.putFloat(null, address + index, v);
        index += 4;
    }

    @Override
    public void writeDouble(double v)
    {
        checkSize(8);
        unsafe.putDouble(null, address + index, v);
        index += 8;
    }

    @Override
    public void writeBytes(String s)
    {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            this.write((byte) s.charAt(i));
        }
    }

    @Override
    public void writeChars(String s)
    {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);
            this.writeChar(v);
        }
    }

    @Override
    public void writeVarInt(int v, boolean optimizeNegativeNumber)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeVarLong(long v, boolean optimizeNegativeNumber)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBoolArray(boolean[] v)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeString(String s)
    {
        throw new UnsupportedOperationException();
    }
}
