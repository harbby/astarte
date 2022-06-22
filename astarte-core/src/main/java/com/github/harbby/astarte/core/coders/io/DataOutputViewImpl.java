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

import static java.util.Objects.requireNonNull;

public class DataOutputViewImpl
        extends OutputStream
        implements DataOutputView
{
    protected final WritableByteChannel channel;
    protected final OutputStream outputStream;
    protected final ByteBuffer byteBuffer;
    protected final byte[] buffer;
    protected int offset;

    public DataOutputViewImpl(WritableByteChannel channel)
    {
        //64k
        this(channel, 1 << 16);
    }

    public DataOutputViewImpl(OutputStream outputStream)
    {
        this(outputStream, 1 << 16);
    }

    public DataOutputViewImpl(OutputStream outputStream, int buffSize)
    {
        //64k
        //this(channel, 1 << 16);
        this.outputStream = requireNonNull(outputStream, "outputStream is null");
        this.channel = null;
        this.buffer = new byte[buffSize];
        this.byteBuffer = ByteBuffer.wrap(buffer);
        this.offset = 0;
    }

    public DataOutputViewImpl(WritableByteChannel channel, int buffSize)
    {
        this.channel = requireNonNull(channel, "channel is null");
        this.outputStream = null;
        this.buffer = new byte[buffSize];
        this.byteBuffer = ByteBuffer.wrap(buffer);
        this.offset = 0;
    }

    @Override
    public void flush()
    {
        try {
            if (outputStream != null) {
                outputStream.write(buffer, 0, offset);
            }
            else {
                byteBuffer.position(0);
                byteBuffer.limit(offset);
                channel.write(byteBuffer);
            }
        }
        catch (IOException e) {
            Throwables.throwThrowable(e);
        }
        this.offset = 0;
    }

    @Override
    public void close()
    {
        try (WritableByteChannel ignored = this.channel;
                OutputStream ignored1 = this.outputStream) {
            this.flush();
        }
        catch (IOException e) {
            Throwables.throwThrowable(e);
        }
    }

    protected void require(int required)
    {
        if (offset + required > buffer.length) {
            this.flush();
        }
    }

    @Override
    public void write(int b)
    {
        require(1);
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
        require(2);
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
        require(4);
        buffer[offset++] = (byte) ((v >>> 24) & 0xFF);
        buffer[offset++] = (byte) ((v >>> 16) & 0xFF);
        buffer[offset++] = (byte) ((v >>> 8) & 0xFF);
        buffer[offset++] = (byte) ((v) & 0xFF);
    }

    @Override
    public void writeLong(long v)
    {
        require(8);
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
        int empSize = buffer.length - offset;
        if (len <= empSize) {
            for (int i = 0; i < len; i++) {
                buffer[offset++] = (byte) (s.charAt(i) & 0X7F);
            }
            return;
        }

        for (int i = 0; i < empSize; i++) {
            buffer[offset++] = (byte) (s.charAt(i) & 0X7F);
        }
        this.flush();
        for (int i = 0; i < len - empSize; i++) {
            buffer[offset++] = (byte) (s.charAt(i) & 0X7F);
        }
    }

    @Override
    public void writeChars(String s)
    {
        //todo: utf16_LE ....
        int len = s.length();
        for (int i = 0; i < len; i++) {
            this.writeChar(s.charAt(i));
        }
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
            require(1);
            buffer[offset++] = (byte) (v & 0x7F);
        }
        else if (v >>> 14 == 0) {
            require(2);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F);
        }
        else if (v >>> 21 == 0) {
            require(3);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F);
        }
        else if (v >>> 28 == 0) {
            require(4);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 21 & 0x7F);
        }
        else {
            require(5);
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
            require(1);
            buffer[offset++] = (byte) (v & 0x7F);
        }
        else if (v >>> 14 == 0) {
            require(2);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F);
        }
        else if (v >>> 21 == 0) {
            require(3);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F);
        }
        else if (v >>> 28 == 0) {
            require(4);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 21 & 0x7F);
        }
        else if (v >>> 35 == 0) {
            require(5);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 21 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 28 & 0x7F);
        }
        else if (v >>> 42 == 0) {
            require(6);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 21 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 28 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 35 & 0x7F);
        }
        else if (v >>> 49 == 0) {
            require(7);
            buffer[offset++] = (byte) (v & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 7 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 14 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 21 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 28 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 35 & 0x7F | 0x80);
            buffer[offset++] = (byte) (v >>> 42 & 0x7F);
        }
        else if (v >>> 56 == 0) {
            require(8);
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
            require(9);
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
            return;
        }
        int byteSize = (value.length + 7) >> 3;
        require(byteSize);
        BoolArrayZipUtil.zip(value, 0, buffer, offset, value.length);
        offset += byteSize;
    }
}
