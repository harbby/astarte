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

import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.collection.StateOption;
import net.jpountz.lz4.LZ4BlockInputStream;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public class EncoderInputStream<E>
        implements Iterator<E>
{
    private final StateOption<E> option = StateOption.empty();
    private final DataInputStream dataInput;
    private final Encoder<E> encoder;
    private boolean closed = false;

    public EncoderInputStream(InputStream inputStream, Encoder<E> encoder)
    {
        this.encoder = requireNonNull(encoder, "encoder is null");
        LZ4BlockInputStream lz4BlockInputStream = new LZ4BlockInputStream(inputStream);
        this.dataInput = new DataInputStream(new BufferedInputStream(lz4BlockInputStream));
        checkState(dataInput.markSupported(), "dataInput not support mark()");
    }

    @Override
    public boolean hasNext()
    {
        if (option.isDefined()) {
            return true;
        }
        if (closed) {
            return false;
        }
        try {
            if (dataInput.available() == 0) {
                dataInput.mark(1);
                if (dataInput.read() == -1) {
                    dataInput.close();
                    this.closed = true;
                    return false;
                }
                dataInput.reset();
            }
            option.update(encoder.decoder(dataInput));
            return true;
        }
        catch (IOException e) {
            throw Throwables.throwsThrowable(e);
        }
    }

    @Override
    public E next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return option.remove();
    }
}
