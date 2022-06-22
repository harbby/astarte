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
import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.collection.IteratorPlus;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public class EncoderInputStream<E>
        implements IteratorPlus<E>, Closeable
{
    private final DataInputView dataInput;
    private final Encoder<E> encoder;
    private final long count;
    private long index = 0;

    public EncoderInputStream(long count, Encoder<E> encoder, DataInputView dataInput)
    {
        checkState(count >= 0, "row count >= 0");
        this.count = count;
        this.encoder = requireNonNull(encoder, "encoder is null");
        this.dataInput = requireNonNull(dataInput, "dataInput is null");
    }

    @Override
    public boolean hasNext()
    {
        boolean hasNext = index < count;
        if (!hasNext) {
            try {
                this.close();
            }
            catch (IOException e) {
                throw Throwables.throwThrowable(e);
            }
        }
        return hasNext;
    }

    @Override
    public E next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        index++;
        return encoder.decoder(dataInput);
    }

    @Override
    public void close()
            throws IOException
    {
        dataInput.close();
    }
}
