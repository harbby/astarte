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

import java.nio.channels.WritableByteChannel;

public final class UnsafeDataOutput
        extends DataOutputViewImpl
{
    private static final Unsafe unsafe = Platform.getUnsafe();

    public UnsafeDataOutput(WritableByteChannel channel, int blockSize)
    {
        super(channel, blockSize);
    }

    @Override
    public void writeBoolean(boolean v)
    {
        require(1);
        unsafe.putBoolean(this.buffer, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, v);
        offset++;
    }

    @Override
    public void writeShort(int v)
    {
        require(2);
        unsafe.putShort(this.buffer, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, (short) v);
        offset += 2;
    }

    @Override
    public void writeChar(int v)
    {
        require(2);
        unsafe.putChar(this.buffer, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, (char) v);
        offset += 2;
    }

    @Override
    public void writeInt(int v)
    {
        require(4);
        unsafe.putInt(this.buffer, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, v);
        offset += 4;
    }

    @Override
    public void writeLong(long v)
    {
        require(8);
        unsafe.putLong(this.buffer, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, v);
        offset += 8;
    }

    @Override
    public void writeFloat(float v)
    {
        require(4);
        unsafe.putFloat(this.buffer, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, v);
        offset += 4;
    }

    @Override
    public void writeDouble(double v)
    {
        require(8);
        unsafe.putDouble(this.buffer, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, v);
        offset += 8;
    }
}
