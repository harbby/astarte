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
package com.github.harbby.astarte.core.coders;

import com.github.harbby.astarte.core.coders.io.DataInputView;
import com.github.harbby.astarte.core.coders.io.DataInputViewImpl;
import com.github.harbby.astarte.core.coders.io.DataOutputView;
import com.github.harbby.astarte.core.coders.io.DataOutputViewImpl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class EncoderChecker<T>
{
    private final Encoder<T> encoder;
    private final byte[] buffer = new byte[4096];
    private final TestByteArrayOutputStream outputStream = new TestByteArrayOutputStream(buffer);
    private final DataOutputView dataOutput = new DataOutputViewImpl(outputStream);
    private final ByteArrayInputStream inputStream = new ByteArrayInputStream(buffer);
    private final DataInputView dataInput = new DataInputViewImpl(inputStream);

    public EncoderChecker(Encoder<T> encoder)
    {
        inputStream.mark(0);
        this.encoder = encoder;
    }

    public byte[] encoder(T value)
            throws IOException
    {
        encoder.encoder(value, dataOutput);
        byte[] bytes = outputStream.toByteArray();
        outputStream.reset();
        Arrays.fill(buffer, (byte) 0);
        return bytes;
    }

    public T decoder(byte[] bytes)
            throws IOException
    {
        System.arraycopy(bytes, 0, buffer, 0, bytes.length);
        T value = encoder.decoder(dataInput);
        inputStream.reset();
        return value;
    }

    private static class TestByteArrayOutputStream
            extends ByteArrayOutputStream
    {
        public TestByteArrayOutputStream(byte[] buffer)
        {
            this.buf = buffer;
        }

        public void reset()
        {
            this.count = 0;
        }
    }
}