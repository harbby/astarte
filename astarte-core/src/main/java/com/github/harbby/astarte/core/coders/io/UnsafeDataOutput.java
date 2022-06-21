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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public final class UnsafeDataOutput
        extends OutputStream
        implements DataOutputView
{
    private static final Unsafe unsafe = Platform.getUnsafe();
    private final byte[] buffer;
    private final WritableByteChannel channel;
    private final ByteBuffer byteBuffer;
    private final int maxOffset;
    private int offset = Unsafe.ARRAY_BYTE_BASE_OFFSET;

    public UnsafeDataOutput(WritableByteChannel channel, int blockSize)
    {
        this.channel = channel;
        buffer = new byte[blockSize];
        this.byteBuffer = ByteBuffer.wrap(buffer);
        this.maxOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + buffer.length;
    }

    private void checkSize(int size)
    {
        if (offset + size > maxOffset) {
            this.flush();
        }
    }

    @Override
    public void flush()
    {
        byteBuffer.limit(offset - Unsafe.ARRAY_BYTE_BASE_OFFSET);
        try {
            channel.write(byteBuffer);
        }
        catch (IOException e) {
            Throwables.throwThrowable(e);
        }
        byteBuffer.clear();
        this.offset = Unsafe.ARRAY_BYTE_BASE_OFFSET;
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
    public void write(int b)
    {
        checkSize(1);
        buffer[offset++] = (byte) b;
    }

    @Override
    public void write(byte[] b)
    {
        this.write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len)
    {
        int index = this.offset - Unsafe.ARRAY_BYTE_BASE_OFFSET;
        int ramming = buffer.length - index;
        while (len > ramming) {
            System.arraycopy(b, off, buffer, index, ramming);
            index = buffer.length;
            this.flush();
            off += ramming;
            len -= ramming;
            ramming = buffer.length;
        }
        System.arraycopy(b, off, buffer, index, len);
        this.offset += len;
    }

    @Override
    public void writeBoolean(boolean v)
    {
        checkSize(1);
        unsafe.putBoolean(this.buffer, (long) offset, v);
        offset++;
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
        unsafe.putShort(this.buffer, (long) offset, (short) v);
        offset += 2;
    }

    @Override
    public void writeChar(int v)
    {
        checkSize(2);
        unsafe.putChar(this.buffer, (long) offset, (char) v);
        offset += 2;
    }

    @Override
    public void writeInt(int v)
    {
        checkSize(4);
        unsafe.putInt(this.buffer, (long) offset, v);
        offset += 4;
    }

    @Override
    public void writeLong(long v)
    {
        checkSize(8);
        unsafe.putLong(this.buffer, (long) offset, v);
        offset += 8;
    }

    @Override
    public void writeFloat(float v)
    {
        checkSize(4);
        unsafe.putFloat(this.buffer, (long) offset, v);
        offset += 4;
    }

    @Override
    public void writeDouble(double v)
    {
        checkSize(8);
        unsafe.putDouble(this.buffer, (long) offset, v);
        offset += 8;
    }

    @Override
    public void writeBytes(String s)
    {
        int len = s.length();
        checkSize(len);
        for (int i = 0; i < len; i++) {
            this.write(s.charAt(i));
        }
    }

    @Override
    public void writeChars(String s)
    {
        int len = s.length();
        checkSize(len * 2);
        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);
            this.writeChar(v);
        }
    }

    @Override
    public void writeVarInt(int v)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeVarLong(long v)
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
