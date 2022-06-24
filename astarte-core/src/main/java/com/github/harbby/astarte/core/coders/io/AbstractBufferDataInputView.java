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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public abstract class AbstractBufferDataInputView
        extends InputStream
        implements DataInputView
{
    protected final byte[] buffer;
    protected int position;
    protected int limit;

    protected AbstractBufferDataInputView(byte[] buffer)
    {
        this.buffer = buffer;
        this.limit = buffer.length;
        this.position = buffer.length;
    }

    protected final void require(int required)
    {
        if (required > buffer.length) {
            throw new RuntimeIOException("buffer is small: capacity: " + buffer.length + ", required: " + required);
        }
        if (limit - position < required) {
            int n = 0;
            if (limit == buffer.length) {
                n = this.refill();
            }
            if (n < required) {
                throw new RuntimeEOFException("required: " + required);
            }
        }
    }

    protected final int refill()
            throws RuntimeIOException
    {
        int l = limit - position;
        if (l > 0) {
            System.arraycopy(buffer, position, buffer, 0, l);
        }
        int n = this.tryReadFully0(buffer, l, position);
        if (n < position) {
            limit = l + n;
        }
        this.position = 0;
        return n;
    }

    protected abstract int tryReadFully0(byte[] b, int off, int len)
            throws RuntimeIOException;

    @Override
    public final int skipBytes(int n)
            throws RuntimeIOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void readFully(byte[] b)
            throws RuntimeIOException
    {
        this.readFully(b, 0, b.length);
    }

    @Override
    public final void readFully(byte[] b, int off, int len)
            throws RuntimeIOException
    {
        int n = this.tryReadFully(b, off, len);
        if (n < len) {
            throw new RuntimeEOFException();
        }
    }

    @Override
    public final int tryReadFully(byte[] b, int off, int len)
            throws RuntimeIOException
    {
        if (limit != buffer.length & position == limit) {
            return -1;
        }
        int rlen = len;
        int index = off;
        do {
            int cacheSize = this.limit - this.position;
            if (cacheSize >= rlen) {
                System.arraycopy(buffer, this.position, b, index, rlen);
                this.position += rlen;
                return index - off + rlen;
            }
            else {
                System.arraycopy(buffer, this.position, b, index, cacheSize);
                this.position = this.limit;
                index += cacheSize;
                rlen -= cacheSize;
                if (limit != buffer.length) {
                    return index - off;
                }
                this.refill();
            }
        }
        while (true);
    }

    @Override
    public int read()
            throws RuntimeIOException
    {
        if (this.position == this.limit) {
            if (limit != buffer.length) {
                return -1;
            }
            else {
                this.refill();
            }
        }
        return buffer[position++] & 0XFF;
    }

    @Override
    public final int read(byte[] b, int off, int len)
            throws RuntimeIOException
    {
        return this.tryReadFully(b, off, len);
    }

    @Override
    public abstract void close();

    @Override
    public boolean readBoolean()
    {
        require(1);
        byte ch = buffer[position++];
        return (ch != 0);
    }

    @Override
    public byte readByte()
    {
        require(1);
        return buffer[position++];
    }

    @Override
    public int readUnsignedByte()
    {
        require(1);
        return buffer[position++] & 0XFF;
    }

    @Override
    public final void readBoolArray(boolean[] booleans, int pos, int len)
    {
        int byteSize = (len + 7) >> 3;
        require(byteSize);
        BoolArrayZipUtil.unzip(buffer, position, booleans, pos, len);
    }

    @Override
    public final int readVarInt(boolean optimizeNegativeNumber)
    {
        require(1);
        byte b = buffer[position++];
        int result = b & 0x7F;
        if (b < 0) {
            require(1);
            b = buffer[position++];
            result |= (b & 0x7F) << 7;
            if (b < 0) {
                require(1);
                b = buffer[position++];
                result |= (b & 0x7F) << 14;
                if (b < 0) {
                    require(1);
                    b = buffer[position++];
                    result |= (b & 0x7F) << 21;
                    if (b < 0) {
                        require(1);
                        b = buffer[position++];
                        //assert b > 0;
                        result |= b << 28;
                    }
                }
            }
        }
        if (optimizeNegativeNumber) {
            return (result >>> 1) ^ -(result & 1);
        }
        else {
            return result;
        }
    }

    @Override
    public final long readVarLong(boolean optimizeNegativeNumber)
    {
        require(1);
        byte b = buffer[position++];
        long result = b & 0x7F;
        if (b < 0) {
            require(1);
            b = buffer[position++];
            result |= (b & 0x7F) << 7;
            if (b < 0) {
                require(1);
                b = buffer[position++];
                result |= (b & 0x7F) << 14;
                if (b < 0) {
                    require(1);
                    b = buffer[position++];
                    result |= (b & 0x7F) << 21;
                    if (b < 0) {
                        require(1);
                        b = buffer[position++];
                        result |= (long) (b & 0x7F) << 28;
                        if (b < 0) {
                            require(1);
                            b = buffer[position++];
                            result |= (long) (b & 0x7F) << 35;
                            if (b < 0) {
                                require(1);
                                b = buffer[position++];
                                result |= (long) (b & 0x7F) << 42;
                                if (b < 0) {
                                    require(1);
                                    b = buffer[position++];
                                    result |= (long) (b & 0x7F) << 49;
                                    if (b < 0) {
                                        require(1);
                                        b = buffer[position++];
                                        result |= (long) b << 56;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        if (optimizeNegativeNumber) {
            return (result >>> 1) ^ -(result & 1);
        }
        else {
            return result;
        }
    }

    private byte[] stringBuffer = new byte[128];

    @Override
    public final String readAsciiString()
    {
        int maxAsciiStringLength = 128;
        int charCount = 0;
        do {
            for (; position < limit; charCount++) {
                if (charCount >= maxAsciiStringLength) {
                    throw new RuntimeEOFException("ascii string max length is " + maxAsciiStringLength);
                }
                byte b = buffer[position++];
                stringBuffer[charCount] = b;
                if (b < 0) {
                    stringBuffer[charCount] &= 0x7F;
                    return new String(stringBuffer, 0, charCount + 1, StandardCharsets.US_ASCII);
                }
            }
            this.refill();
        }
        while (true);
    }

    @Override
    public final String readString()
    {
        require(1);
        byte b = buffer[position];
        if (b == (byte) 0x80) {
            position++;
            return null;
        }
        else if (b == (byte) 0x81) {
            position++;
            return "";
        }
        else if ((b & 0x80) == 0) {
            // ascii
            return readAsciiString();
        }
        int len = readUtf8VarLength() - 1;
        if (stringBuffer.length < len) {
            stringBuffer = new byte[len];
        }
        this.tryReadFully(stringBuffer, 0, len);
        return new String(stringBuffer, 0, len, StandardCharsets.UTF_8);
    }

    private int readUtf8VarLength()
    {
        require(1);
        byte b = buffer[position++];
        int result = b & 0x7F;
        assert b < 0;
        if ((b & 0x60) == 0) {
            return b & 0x3F;
        }
        require(1);
        b = buffer[position++];
        result |= (b & 0x7F) << 7;
        if (b < 0) {
            require(1);
            b = buffer[position++];
            result |= (b & 0x7F) << 14;
            if (b < 0) {
                require(1);
                b = buffer[position++];
                result |= (b & 0x7F) << 21;
                if (b < 0) {
                    require(1);
                    b = buffer[position++];
                    //assert b > 0;
                    result |= b << 28;
                }
            }
        }
        return result;
    }
}
