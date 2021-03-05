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

import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class Tuple2Encoder<K, V>
        implements Encoder<Tuple2<K, V>>
{
    private final Encoder<K> kEncoder;
    private final Encoder<V> vEncoder;

    public Encoder<K> getKeyEncoder()
    {
        return kEncoder;
    }

    public Encoder<V> getValueEncoder()
    {
        return vEncoder;
    }

    public Tuple2Encoder(Encoder<K> kEncoder, Encoder<V> vEncoder)
    {
        this.kEncoder = kEncoder;
        this.vEncoder = vEncoder;
    }

    @Override
    public void encoder(Tuple2<K, V> value, DataOutput output)
            throws IOException
    {
        requireNonNull(value, "Tuple2 value is null");
        kEncoder.encoder(value.f1, output);
        vEncoder.encoder(value.f2, output);
    }

    @Override
    public Tuple2<K, V> decoder(DataInput input)
            throws IOException
    {
        return new Tuple2<>(kEncoder.decoder(input), vEncoder.decoder(input));
    }

    @Override
    public Comparator<Tuple2<K, V>> comparator()
    {
        return (kv1, kv2) -> {
            int than = kEncoder.comparator().compare(kv1.f1, kv2.f1);
            if (than != 0) {
                return than;
            }
            return vEncoder.comparator().compare(kv1.f2, kv2.f2);
        };
    }
}
