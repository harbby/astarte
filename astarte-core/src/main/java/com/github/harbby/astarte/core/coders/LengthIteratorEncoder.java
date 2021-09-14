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

import com.github.harbby.astarte.core.api.LengthIterator;
import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.gadtry.base.Throwables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public class LengthIteratorEncoder<E>
        implements Encoder<Iterator<E>>
{
    private final Encoder<E> eEncoder;

    public LengthIteratorEncoder(Encoder<E> eEncoder)
    {
        this.eEncoder = requireNonNull(eEncoder, "eEncoder is null");
    }

    @Override
    public void encoder(Iterator<E> value, DataOutput output)
            throws IOException
    {
        checkState(value instanceof LengthIterator, "only support LengthIterator");
        output.writeInt(((LengthIterator<?>) value).length());
        while (value.hasNext()) {
            eEncoder.encoder(value.next(), output);
        }
    }

    @Override
    public LengthIterator<E> decoder(DataInput input)
            throws IOException
    {
        final int length = input.readInt();
        return new LengthIterator<E>()
        {
            private int index = 0;

            @Override
            public int length()
            {
                return length;
            }

            @Override
            public boolean hasNext()
            {
                return index < length;
            }

            @Override
            public E next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                index++;
                try {
                    return eEncoder.decoder(input);
                }
                catch (IOException e) {
                    throw Throwables.throwThrowable(e);
                }
            }
        };
    }

    @Override
    public Comparator<Iterator<E>> comparator()
    {
        throw new UnsupportedOperationException();
    }
}
