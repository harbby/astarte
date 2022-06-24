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
import java.io.IOException;

public interface DataInputView
        extends DataInput, Closeable
{
    @Override
    void readFully(byte[] b) throws RuntimeIOException;

    @Override
    void readFully(byte[] b, int off, int len) throws RuntimeIOException;

    int tryReadFully(byte[] b, int off, int len) throws RuntimeIOException;

    /**
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an {@code int} in the range {@code 0} to
     * {@code 255}. If no byte is available because the end of the stream
     * has been reached, the value {@code -1} is returned. This method
     * blocks until input data is available, the end of the stream is detected,
     * or an exception is thrown.
     *
     * <p> A subclass must provide an implementation of this method.
     *
     * @return     the next byte of data, or {@code -1} if the end of the
     *             stream is reached.
     * @throws     IOException  if an I/O error occurs.
     */
    int read() throws RuntimeIOException;

    @Override
    int skipBytes(int n) throws RuntimeIOException;

    @Override
    boolean readBoolean() throws RuntimeIOException;

    @Override
    byte readByte() throws RuntimeIOException;

    @Override
    int readUnsignedByte() throws RuntimeIOException;

    @Override
    short readShort() throws RuntimeIOException;

    @Override
    int readUnsignedShort() throws RuntimeIOException;

    @Override
    char readChar() throws RuntimeIOException;

    @Override
    int readInt() throws RuntimeIOException;

    @Override
    long readLong() throws RuntimeIOException;

    @Override
    float readFloat() throws RuntimeIOException;

    @Override
    double readDouble() throws RuntimeIOException;

    @Deprecated
    @Override
    default String readLine() throws RuntimeIOException
    {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    default String readUTF() throws RuntimeIOException
    {
        throw new UnsupportedOperationException();
    }

    String readAsciiString() throws RuntimeIOException;

    String readString() throws RuntimeIOException;

    void readBoolArray(boolean[] booleans, int pos, int len) throws RuntimeIOException;

    int readVarInt(boolean optimizeNegativeNumber) throws RuntimeIOException;

    long readVarLong(boolean optimizeNegativeNumber) throws RuntimeIOException;

    @Override
    void close() throws RuntimeIOException;
}
