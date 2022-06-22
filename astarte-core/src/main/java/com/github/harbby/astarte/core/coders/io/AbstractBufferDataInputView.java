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

import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public abstract class AbstractBufferDataInputView
        implements DataInputView
{
    protected final byte[] buffer;
    protected int offset;
    protected int end;

    protected AbstractBufferDataInputView(byte[] buffer)
    {
        this.buffer = buffer;
        this.end = buffer.length;
        this.offset = buffer.length;
    }

    protected void require(int required)
    {
        if (end - offset < required) {
            try {
                int n = this.refill();
                if (n < required) {
                    Throwables.throwThrowable(new EOFException("required: " + required));
                }
            }
            catch (IOException e) {
                Throwables.throwThrowable(e);
            }
        }
    }

    protected int refill()
            throws IOException
    {
        if (end != buffer.length) {
            throw new EOFException();
        }
        int l = end - offset;
        if (l > 0) {
            System.arraycopy(buffer, offset, buffer, 0, l);
        }
        int n = this.tryReadFully0(buffer, l, offset);
        if (n < offset) {
            end = l + n;
        }
        this.offset = 0;
        return n;
    }

    protected abstract int tryReadFully0(byte[] b, int off, int len)
            throws IOException;

    protected abstract void readFully0(byte[] b, int off, int len)
            throws IOException;

    protected abstract int skipBytes0(int n)
            throws IOException;

    @Override
    public int skipBytes(int n)
            throws IOException
    {
        throw new UnsupportedEncodingException();
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
        int cacheSize = end - offset;
        if (len <= cacheSize) {
            System.arraycopy(buffer, offset, b, off, len);
            offset += len;
            return;
        }
        System.arraycopy(buffer, offset, b, off, cacheSize);
        offset = end;
        this.refill();
        if (end == buffer.length) {
            //todo end > len -cacheSize
            System.arraycopy(buffer, 0, b, cacheSize, len - cacheSize);
        }
        else {
            System.arraycopy(buffer, 0, b, cacheSize, end);
            throw new EOFException();
        }
    }

    @Override
    public int tryReadFully(byte[] b, int off, int len)
            throws IOException
    {
        int cacheSize = end - offset;
        if (len <= cacheSize) {
            System.arraycopy(buffer, offset, b, off, len);
            offset += len;
            return len;
        }
        System.arraycopy(buffer, offset, b, off, cacheSize);
        offset = end;
        this.refill();
        if (end == buffer.length) {
            //todo end > len -cacheSize
            System.arraycopy(buffer, 0, b, cacheSize, len - cacheSize);
            return len;
        }
        else {
            System.arraycopy(buffer, 0, b, cacheSize, end);
            return cacheSize + end;
        }
    }

    @Override
    public boolean readBoolean()
    {
        require(1);
        byte ch = buffer[offset++];
        return (ch != 0);
    }

    @Override
    public byte readByte()
    {
        require(1);
        return buffer[offset++];
    }

    @Override
    public int readUnsignedByte()
    {
        require(1);
        return buffer[offset++] & 0XFF;
    }

    @Override
    public void readBoolArray(boolean[] booleans, int pos, int len)
    {
        int byteSize = (len + 7) >> 3;
        require(byteSize);
        BoolArrayZipUtil.unzip(buffer, offset, booleans, pos, len);
    }

    @Override
    public int readVarInt(boolean optimizeNegativeNumber)
    {
        require(1);
        byte b = buffer[offset++];
        int result = b & 0x7F;
        if (b < 0) {
            require(1);
            b = buffer[offset++];
            result |= (b & 0x7F) << 7;
            if (b < 0) {
                require(1);
                b = buffer[offset++];
                result |= (b & 0x7F) << 14;
                if (b < 0) {
                    require(1);
                    b = buffer[offset++];
                    result |= (b & 0x7F) << 21;
                    if (b < 0) {
                        require(1);
                        b = buffer[offset++];
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
    public long readVarLong(boolean optimizeNegativeNumber)
    {
        require(1);
        byte b = buffer[offset++];
        long result = b & 0x7F;
        if (b < 0) {
            require(1);
            b = buffer[offset++];
            result |= (b & 0x7F) << 7;
            if (b < 0) {
                require(1);
                b = buffer[offset++];
                result |= (b & 0x7F) << 14;
                if (b < 0) {
                    require(1);
                    b = buffer[offset++];
                    result |= (b & 0x7F) << 21;
                    if (b < 0) {
                        require(1);
                        b = buffer[offset++];
                        result |= (long) (b & 0x7F) << 28;
                        if (b < 0) {
                            require(1);
                            b = buffer[offset++];
                            result |= (long) (b & 0x7F) << 35;
                            if (b < 0) {
                                require(1);
                                b = buffer[offset++];
                                result |= (long) (b & 0x7F) << 42;
                                if (b < 0) {
                                    require(1);
                                    b = buffer[offset++];
                                    result |= (long) (b & 0x7F) << 49;
                                    if (b < 0) {
                                        require(1);
                                        b = buffer[offset++];
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
}
