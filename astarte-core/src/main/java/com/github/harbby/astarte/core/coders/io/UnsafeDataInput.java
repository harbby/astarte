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
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.InputStream;

public final class UnsafeDataInput
        extends AbstractBufferDataInputView
{
    private static final Unsafe unsafe = Platform.getUnsafe();
    private final InputStream in;

    public UnsafeDataInput(InputStream in)
    {
        super(new byte[1 << 16]);
        this.in = in;
    }

    @Override
    protected int tryReadFully0(byte[] b, int off, int len)
            throws RuntimeIOException
    {
        try {
            return IoUtils.tryReadFully(in, b, off, len);
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public short readShort()
    {
        require(2);
        short v = unsafe.getShort(buffer, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + position);
        position += 2;
        return v;
    }

    @Override
    public int readUnsignedShort()
    {
        require(2);
        short v = unsafe.getShort(buffer, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + position);
        position += 2;
        return v & 0XFFFF;
    }

    @Override
    public char readChar()
    {
        require(2);
        char v = unsafe.getChar(buffer, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + position);
        position += 2;
        return v;
    }

    @Override
    public int readInt()
    {
        require(4);
        int v = unsafe.getInt(buffer, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + position);
        position += 4;
        return v;
    }

    @Override
    public long readLong()
    {
        require(8);
        long l = unsafe.getLong(buffer, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + position);
        position += 8;
        return l;
    }

    @Override
    public float readFloat()
    {
        require(4);
        float v = unsafe.getFloat(buffer, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + position);
        position += 4;
        return v;
    }

    @Override
    public double readDouble()
    {
        require(8);
        double v = unsafe.getDouble(buffer, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + position);
        position += 8;
        return v;
    }

    @Override
    public void close()
    {
        try {
            this.in.close();
        }
        catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }
}
