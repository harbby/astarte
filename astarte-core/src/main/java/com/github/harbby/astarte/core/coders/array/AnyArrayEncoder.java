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
package com.github.harbby.astarte.core.coders.array;

import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.astarte.core.coders.Encoder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnyArrayEncoder<E>
        implements Encoder<E[]>
{
    private final Encoder<E> encoder;
    private final Class<E> classTag;

    public AnyArrayEncoder(Encoder<E> encoder, Class<E> classTag)
    {
        this.encoder = encoder;
        this.classTag = classTag;
    }

    @Override
    public void encoder(E[] values, DataOutput output)
            throws IOException
    {
        if (values == null) {
            output.writeInt(-1);
            return;
        }
        output.writeInt(values.length);
        for (E e : values) {
            encoder.encoder(e, output);
        }
    }

    @Override
    public E[] decoder(DataInput input)
            throws IOException
    {
        int len = input.readInt();
        if (len == -1) {
            return null;
        }
        @SuppressWarnings("unchecked")
        E[] values = (E[]) java.lang.reflect.Array.newInstance(classTag, len);
        for (int i = 0; i < len; i++) {
            values[i] = encoder.decoder(input);
        }
        return values;
    }

    @Override
    public Comparator<E[]> comparator()
    {
        return comparator(encoder.comparator());
    }

    public static <E> Comparator<E[]> comparator(Comparator<E> comparator)
    {
        return (arr1, arr2) -> {
            int len1 = arr1.length;
            int len2 = arr2.length;
            int lim = Math.min(len1, len2);

            int k = 0;
            while (k < lim) {
                E c1 = arr1[k];
                E c2 = arr2[k];
                if (c1 != c2) {
                    return comparator.compare(c1, c2);
                }
                k++;
            }
            return len1 - len2;
        };
    }
}
