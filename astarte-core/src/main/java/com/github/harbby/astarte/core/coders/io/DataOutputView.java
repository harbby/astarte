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
import java.io.DataOutput;

public interface DataOutputView
        extends DataOutput, Closeable
{
    @Override
    void writeInt(int v)
            throws RuntimeIOException;

    @Override
    void write(int b)
            throws RuntimeIOException;

    @Override
    void write(byte[] b)
            throws RuntimeIOException;

    @Override
    void write(byte[] b, int off, int len)
            throws RuntimeIOException;

    @Override
    void writeBoolean(boolean v)
            throws RuntimeIOException;

    @Override
    void writeByte(int v)
            throws RuntimeIOException;

    @Override
    void writeShort(int v)
            throws RuntimeIOException;

    @Override
    void writeChar(int v)
            throws RuntimeIOException;

    @Override
    void writeLong(long v)
            throws RuntimeIOException;

    @Override
    void writeFloat(float v)
            throws RuntimeIOException;

    @Override
    void writeDouble(double v)
            throws RuntimeIOException;

    /**
     * code: Latin-1
     */
    @Deprecated
    @Override
    void writeBytes(String s)
            throws RuntimeIOException;

    @Override
    void writeChars(String s)
            throws RuntimeIOException;

    @Deprecated
    @Override
    default void writeUTF(String s)
            throws RuntimeIOException
    {
        this.writeString(s);
    }

    void writeVarInt(int v, boolean optimizeNegativeNumber)
            throws RuntimeIOException;

    void writeVarLong(long v, boolean optimizeNegativeNumber)
            throws RuntimeIOException;

    void writeBoolArray(boolean[] v)
            throws RuntimeIOException;

    void writeAsciiString(String s)
            throws RuntimeEOFException;

    void writeString(String s)
            throws RuntimeIOException;

    void flush()
            throws RuntimeIOException;

    @Override
    void close()
            throws RuntimeIOException;
}
