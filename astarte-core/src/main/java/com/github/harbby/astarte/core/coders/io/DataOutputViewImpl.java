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

import com.github.harbby.gadtry.base.Throwables;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

public class DataOutputViewImpl
        extends OutputStream
        implements DataOutputView
{
    protected final WritableByteChannel channel;
    protected final ByteBuffer byteBuffer;
    protected final byte[] buffer;
    protected int offset;

    public DataOutputViewImpl(WritableByteChannel channel)
    {
        //64k
        this(channel, 1 << 16);
    }

    public DataOutputViewImpl(WritableByteChannel channel, int buffSize)
    {
        this.channel = channel;
        this.buffer = new byte[buffSize];
        this.byteBuffer = ByteBuffer.wrap(buffer);
        this.offset = 0;
    }

    @Override
    public void flush()
    {
        byteBuffer.limit(offset);
        try {
            channel.write(byteBuffer);
        }
        catch (IOException e) {
            Throwables.throwThrowable(e);
        }
        byteBuffer.clear();
        this.offset = 0;
    }

    protected void checkSize(int size)
    {
        if (offset + size > buffer.length) {
            this.flush();
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
        int ramming = buffer.length - offset;
        while (len > ramming) {
            System.arraycopy(b, off, buffer, offset, ramming);
            offset = buffer.length;
            this.flush();
            off += ramming;
            len -= ramming;
            ramming = buffer.length;
        }
        System.arraycopy(b, off, buffer, offset, len);
        offset += len;
    }

    @Override
    public void writeBoolean(boolean v)
    {
        this.writeByte(v ? 1 : 0);
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
        buffer[offset++] = (byte) ((v >>> 8) & 0xFF);
        buffer[offset++] = (byte) ((v) & 0xFF);
    }

    @Override
    public void writeChar(int v)
    {
        this.writeShort(v);
    }

    @Override
    public void writeInt(int v)
    {
        checkSize(4);
        buffer[offset++] = (byte) ((v >>> 24) & 0xFF);
        buffer[offset++] = (byte) ((v >>> 16) & 0xFF);
        buffer[offset++] = (byte) ((v >>> 8) & 0xFF);
        buffer[offset++] = (byte) ((v) & 0xFF);
    }

    @Override
    public void writeLong(long v)
    {
        checkSize(8);
        buffer[offset++] = (byte) (v >>> 56);
        buffer[offset++] = (byte) (v >>> 48);
        buffer[offset++] = (byte) (v >>> 40);
        buffer[offset++] = (byte) (v >>> 32);
        buffer[offset++] = (byte) (v >>> 24);
        buffer[offset++] = (byte) (v >>> 16);
        buffer[offset++] = (byte) (v >>> 8);
        buffer[offset++] = (byte) (v);
    }

    @Override
    public void writeFloat(float v)
    {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeDouble(double v)
    {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(String s)
    {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            this.write((byte) s.charAt(i));
        }
        offset += len;
    }

    @Override
    public void writeChars(String s)
    {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            this.writeChar(s.charAt(i));
        }
        offset += len * 2;
    }

    @Override
    public void writeString(String s)
    {
        //dataOutputStream.writeUTF(s);
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeVarInt(int value, boolean optimizeNegativeNumber)
    {
        int v = value;
        if (optimizeNegativeNumber) {
            // zigzag coder: v = v >=0 ? value << 1 : (~value) + 1;
            v = (v << 1) ^ (v >> 31);
        }
        if (v >>> 7 == 0) {
            checkSize(1);
            buffer[offset++] = (byte) (v & 0x7F);
        }
        else if (v >>> 14 == 0) {
            checkSize(2);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F);
        }
        else if (v >>> 21 == 0) {
            checkSize(3);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F);
        }
        else if (v >>> 28 == 0) {
            checkSize(4);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 21 & 0x7F);
        }
        else {
            checkSize(5);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 21 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 28);
        }
    }

    @Override
    public void writeVarLong(long value, boolean optimizeNegativeNumber)
    {
        long v = value;
        if (optimizeNegativeNumber) {
            // zigzag coder: v = v >=0 ? value << 1 : (~value) + 1;
            v = (v << 1) ^ (v >> 63);
        }
        if (v >>> 7 == 0) {
            checkSize(1);
            buffer[offset++] = (byte) (v & 0x7F);
        }
        else if (v >>> 14 == 0) {
            checkSize(2);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F);
        }
        else if (v >>> 21 == 0) {
            checkSize(3);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F);
        }
        else if (v >>> 28 == 0) {
            checkSize(4);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 21 & 0x7F);
        }
        else if (v >>> 35 == 0) {
            checkSize(5);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 21 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 28 & 0x7F);
        }
        else if (v >>> 42 == 0) {
            checkSize(6);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 21 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 28 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 35 & 0x7F);
        }
        else if (v >>> 49 == 0) {
            checkSize(7);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 21 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 28 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 35 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 42 & 0x7F);
        }
        else if (v >>> 56 == 0) {
            checkSize(8);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 21 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 28 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 35 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 42 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 49 & 0x7F);
        }
        else {
            checkSize(9);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 21 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 28 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 35 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 42 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 49 & 0x7F | 0x80);
            // remain 8bit (64 - 8 * 7)
            buffer[offset++] = (byte) (v >>> 56);
        }
    }

    @Override
    public void writeBoolArray(boolean[] value)
    {
        if (value.length == 0) {
            this.writeVarInt(0, false);
            return;
        }
        this.writeVarInt(value.length, false);
        int byteSize = (value.length + 7) >> 3;
        checkSize(byteSize);

        zip(value, 0, buffer, offset, value.length);
        offset += byteSize;
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

    public static void zip(boolean[] src, int srcPos, byte[] dest, int destPos, int dataLength)
    {
        int byteSize = (dataLength + 7) >> 3;
        Arrays.fill(dest, destPos, destPos + byteSize, (byte) 0);
        for (int i = 0; i < dataLength; i++) {
            if (src[i + srcPos]) {
                dest[(i >> 3) + destPos] |= 0x80 >> (i & 7);
            }
        }
    }

    public static void unzip(byte[] src, int srcPos, boolean[] dest, int destPos, int dataLength)
    {
        for (int i = 0; i < dataLength; i++) {
            byte v = src[(i >> 3) + srcPos];
            dest[i + destPos] = (v & (0x80 >> (i & 7))) != 0;
        }
    }
}
