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

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
            throw new RuntimeIOException(e);
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
            throw new RuntimeIOException(e);
        }
    }

    protected void require(int required)
    {
        int ramming = buffer.length - offset;
        if (required > ramming) {
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
            throws RuntimeIOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void writeAsciiString(String s)
            throws RuntimeEOFException
    {
        if (s == null) {
            throw new RuntimeIOException("Encoding ascii string, str is null.");
        }
        int len = s.length();
        if (len == 0) {
            throw new RuntimeIOException("Encoding ascii string, str.length() is zero.");
        }
        this.writeAscii0(s, len);
    }

    private void writeAscii0(String s, int len)
            throws RuntimeEOFException
    {
        assert len > 0;
        int ramming = buffer.length - offset;
        int off = 0;
        while (len > ramming) {
            s.getBytes(off, off + ramming, buffer, offset);
            for (int i = 0; i < ramming; i++) {
                buffer[offset++] &= 0x7F;
            }
            assert offset == buffer.length;
            this.flush();
            off += ramming;
            len -= ramming;
            ramming = buffer.length;
        }
        s.getBytes(off, off + len, buffer, offset);
        for (int i = 0; i < len - 1; i++) {
            buffer[offset++] &= 0x7F;
        }
        buffer[offset++] |= 0x80;
    }

    @Override
    public void writeChars(String s)
    {
        //todo: utf16_LE ....
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeString(String s)
    {
        if (s == null) {
            require(1);
            buffer[offset++] = (byte) 0x80;
            return;
        }
        int len = s.length();
        if (len == 0) {
            require(1);
            buffer[offset++] = (byte) 0x81;
            return;
        }

        if (len > 1 && len <= 128 && isAscii(s, len)) {
            writeAscii0(s, len);
            return;
        }
        // write utf-8 length 2 bytes
        if (len > 65535) {
            throw new RuntimeEOFException("utf-8 length too long 65535 bytes");
        }
        writeUtf8VarLength(len + 1);
        writeUtf8(s, len);
        /**
         * @see java.io.DataOutputStream#writeUTF(String)
         * */
    }

    private void writeUtf8(String str, int len)
    {
        for (int i = 0; i < len; i++) {
            char ch = str.charAt(i);
            if (ch < 0x80 && ch != 0) {
                require(1);
                buffer[offset++] = (byte) ch;
            }
            else if (ch >= 0x800) {
                require(3);
                buffer[offset++] = (byte) (0xE0 | ((ch >> 12) & 0x0F));
                buffer[offset++] = (byte) (0x80 | ((ch >> 6) & 0x3F));
                buffer[offset++] = (byte) (0x80 | ((ch) & 0x3F));
            }
            else {
                require(2);
                buffer[offset++] = (byte) (0xC0 | ((ch >> 6) & 0x1F));
                buffer[offset++] = (byte) (0x80 | ((ch) & 0x3F));
            }
        }
    }

    private void writeUtf8VarLength(int len)
    {
        if (len >>> 7 == 0) {
            require(1);
            buffer[offset++] = (byte) (len | 0x80);
            return;
        }
        else if (len >>> 14 == 0) {
            throw new UnsupportedOperationException();
        }
        throw new UnsupportedOperationException();
    }

    private static boolean isAscii(String s, int len)
    {
        try {
            //java11+
            Method method = String.class.getDeclaredMethod("isLatin1");
            method.setAccessible(true);
            return (boolean) method.invoke(s);
        }
        catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeEOFException(e);
        }
    }

    @Override
    public final void writeVarInt(int value, boolean optimizeNegativeNumber)
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
    public final void writeVarLong(long value, boolean optimizeNegativeNumber)
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
    public final void writeBoolArray(boolean[] value)
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
