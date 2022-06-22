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
import java.io.InputStream;

public final class DataInputViewImpl
        extends AbstractBufferDataInputView
{
    private final InputStream in;

    public DataInputViewImpl(InputStream in)
    {
        super(new byte[1 << 16]);
        this.in = in;
    }

    @Override
    public void close()
    {
        try {
            this.in.close();
        }
        catch (IOException e) {
            Throwables.throwThrowable(e);
        }
    }

    @Override
    public void readFully(byte[] b)
            throws IOException
    {
        this.readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len)
            throws IOException
    {
        IoUtils.readFully(in, b, off, len);
    }

    @Override
    public int tryReadFully(byte[] b, int off, int len)
            throws IOException
    {
        return IoUtils.tryReadFully(in, b, off, len);
    }

    @Override
    public int skipBytes(int n)
            throws IOException
    {
        return IoUtils.skipBytes(in, n);
    }

    @Override
    public short readShort()
    {
        require(2);
        int ch1 = buffer[offset++] & 0XFF;
        int ch2 = buffer[offset++] & 0XFF;
        return (short) ((ch1 << 8) + ch2);
    }

    @Override
    public int readUnsignedShort()
    {
        require(2);
        int ch1 = buffer[offset++] & 0XFF;
        int ch2 = buffer[offset++] & 0XFF;
        return (ch1 << 8) + ch2;
    }

    @Override
    public char readChar()
    {
        require(2);
        int ch1 = buffer[offset++] & 0XFF;
        int ch2 = buffer[offset++] & 0XFF;
        return (char) ((ch1 << 8) + ch2);
    }

    @Override
    public int readInt()
    {
        require(4);
        int ch1 = buffer[offset++] & 0XFF;
        int ch2 = buffer[offset++] & 0XFF;
        int ch3 = buffer[offset++] & 0XFF;
        int ch4 = buffer[offset++] & 0XFF;
        return (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4;
    }

    @Override
    public long readLong()
    {
        require(8);
        return (((long) buffer[offset++] << 56) +
                ((long) (buffer[offset++] & 255) << 48) +
                ((long) (buffer[offset++] & 255) << 40) +
                ((long) (buffer[offset++] & 255) << 32) +
                ((long) (buffer[offset++] & 255) << 24) +
                ((buffer[offset++] & 255) << 16) +
                ((buffer[offset++] & 255) << 8) +
                ((buffer[offset++] & 255)));
    }

    @Override
    public float readFloat()
    {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble()
    {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public String readString()
    {
        return null;
    }
}
