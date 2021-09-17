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

import java.io.Closeable;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public final class UnsafeDataInput
        implements DataInput, Closeable
{
    private final InputStream in;

    public UnsafeDataInput(InputStream in)
    {
        this.in = in;
    }

    @Override
    public void close()
            throws IOException
    {
        in.close();
    }

    @Override
    public void readFully(byte[] b)
            throws IOException
    {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len)
            throws IOException
    {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }
        int n = 0;
        while (n < len) {
            int count = in.read(b, off + n, len - n);
            if (count < 0) {
                throw new EOFException();
            }
            n += count;
        }
    }

    @Override
    public int skipBytes(int n)
            throws IOException
    {
        int total = 0;
        int cur = 0;

        while ((total < n) && ((cur = (int) in.skip(n - total)) > 0)) {
            total += cur;
        }

        return total;
    }

    @Override
    public boolean readBoolean()
            throws IOException
    {
        int ch = in.read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (ch != 0);
    }

    @Override
    public byte readByte()
            throws IOException
    {
        int ch = in.read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (byte) (ch);
    }

    @Override
    public int readUnsignedByte()
            throws IOException
    {
        int ch = in.read();
        if (ch < 0) {
            throw new EOFException();
        }
        return ch;
    }

    @Override
    public short readShort()
            throws IOException
    {
        int ch2 = in.read();
        int ch1 = in.read();
        if ((ch1 | ch2) < 0) {
            throw new EOFException();
        }
        return (short) ((ch1 << 8) + (ch2 << 0));
    }

    @Override
    public int readUnsignedShort()
            throws IOException
    {
        int ch2 = in.read();
        int ch1 = in.read();
        if ((ch1 | ch2) < 0) {
            throw new EOFException();
        }
        return (ch1 << 8) + (ch2 << 0);
    }

    @Override
    public char readChar()
            throws IOException
    {
        int ch2 = in.read();
        int ch1 = in.read();
        if ((ch1 | ch2) < 0) {
            throw new EOFException();
        }
        return (char) ((ch1 << 8) + (ch2 << 0));
    }

    @Override
    public int readInt()
            throws IOException
    {
        int ch4 = in.read();
        int ch3 = in.read();
        int ch2 = in.read();
        int ch1 = in.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0) {
            throw new EOFException();
        }
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    private final byte[] readBuffer = new byte[8];

    @Override
    public long readLong()
            throws IOException
    {
        readFully(readBuffer, 0, 8);
        return (((long) readBuffer[7] << 56) +
                ((long) (readBuffer[6] & 255) << 48) +
                ((long) (readBuffer[5] & 255) << 40) +
                ((long) (readBuffer[4] & 255) << 32) +
                ((long) (readBuffer[3] & 255) << 24) +
                ((readBuffer[2] & 255) << 16) +
                ((readBuffer[1] & 255) << 8) +
                ((readBuffer[0] & 255) << 0));
    }

    @Override
    public float readFloat()
            throws IOException
    {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble()
            throws IOException
    {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public String readLine()
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF()
            throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
