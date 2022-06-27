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
import java.util.Arrays;

public class EncoderChecker<T>
{
    private final Encoder<T> encoder;
    private final TestByteArrayOutputStream outputStream = new TestByteArrayOutputStream();
    private final TestByteArrayInputStream inputStream = new TestByteArrayInputStream();

    public EncoderChecker(Encoder<T> encoder)
    {
        inputStream.mark(0);
        this.encoder = encoder;
    }

    public byte[] encoder(T value)
    {
        DataOutputView dataOutput = new DataOutputViewImpl(outputStream);
        encoder.encoder(value, dataOutput);
        dataOutput.close();
        byte[] bytes = outputStream.toByteArray();
        outputStream.reset();
        return bytes;
    }

    public T decoder(byte[] bytes)
    {
        inputStream.reset(bytes);
        DataInputView dataInput = new DataInputViewImpl(inputStream);
        T value = encoder.decoder(dataInput);
        return value;
    }

    private static final class TestByteArrayOutputStream
            extends ByteArrayOutputStream
    {
        public TestByteArrayOutputStream()
        {
            super(16);
        }

        public void reset()
        {
            Arrays.fill(this.buf, 0, count, (byte) 0);
            this.count = 0;
        }
    }

    private static final class TestByteArrayInputStream
            extends ByteArrayInputStream
    {
        public TestByteArrayInputStream()
        {
            super(new byte[0]);
        }

        public void reset(byte[] bytes)
        {
            this.buf = bytes;
            super.count = bytes.length;
            super.pos = 0;
        }
    }
}
