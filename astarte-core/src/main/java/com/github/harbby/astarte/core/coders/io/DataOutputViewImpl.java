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

public final class DataOutputViewImpl
        extends OutputStream
        implements DataOutputView
{
    private final WritableByteChannel channel;
    private final ByteBuffer byteBuffer;
    private final byte[] buffer;
    private int index = 0;

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
    }

    @Override
    public void flush()
    {
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

    private void checkSize(int size)
    {
        if (index + size > buffer.length) {
            this.flush();
        }
    }

    @Override
    public void write(int b)
    {
        checkSize(1);
        buffer[index++] = (byte) b;
    }

    @Override
    public void write(byte[] b)
    {
        this.write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len)
    {
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
        index += len;
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
        buffer[index++] = (byte) ((v >>> 8) & 0xFF);
        buffer[index++] = (byte) ((v) & 0xFF);
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
        buffer[index++] = (byte) ((v >>> 24) & 0xFF);
        buffer[index++] = (byte) ((v >>> 16) & 0xFF);
        buffer[index++] = (byte) ((v >>> 8) & 0xFF);
        buffer[index++] = (byte) ((v) & 0xFF);
    }

    @Override
    public void writeLong(long v)
    {
        checkSize(8);
        buffer[index++] = (byte) (v >>> 56);
        buffer[index++] = (byte) (v >>> 48);
        buffer[index++] = (byte) (v >>> 40);
        buffer[index++] = (byte) (v >>> 32);
        buffer[index++] = (byte) (v >>> 24);
        buffer[index++] = (byte) (v >>> 16);
        buffer[index++] = (byte) (v >>> 8);
        buffer[index++] = (byte) (v);
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
        index += len;
    }

    @Override
    public void writeChars(String s)
    {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            this.writeChar(s.charAt(i));
        }
        index += len * 2;
    }

    @Override
    public void writeString(String s)
    {
        //dataOutputStream.writeUTF(s);
        throw new UnsupportedOperationException();
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
    public void close()
    {
        try (WritableByteChannel ignored = this.channel) {
            this.flush();
        }
        catch (IOException e) {
            Throwables.throwThrowable(e);
        }
    }
}
