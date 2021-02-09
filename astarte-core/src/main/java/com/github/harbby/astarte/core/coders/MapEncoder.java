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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author ivan
 * @date 2021.02.07 21:34:23
 * map Serialize
 */
public class MapEncoder<K, V>
        implements Encoder<Map<K, V>>
{
    private final Encoder<K> kEncoder;
    private final Encoder<V> vEncoder;

    /**
     * encode map
     *
     * @param kEncoder
     * @param vEncoder
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> Encoder<Map<K, V>> mapEncoder(Encoder<K> kEncoder, Encoder<V> vEncoder)
    {
        requireNonNull(kEncoder, "key Encoder is null");
        requireNonNull(vEncoder, "value Encoder is null");
        return new MapEncoder<>(kEncoder, vEncoder);
    }

    private MapEncoder(Encoder<K> kEncoder, Encoder<V> vEncoder)
    {
        this.kEncoder = kEncoder;
        this.vEncoder = vEncoder;
    }

    @Override
    public void encoder(Map<K, V> value, DataOutput output) throws IOException
    {
        final int size = value.size();
        //write size on the head
        output.writeInt(size);
        //write key and value
        for (Map.Entry<K, V> entry :
                value.entrySet()) {
            K k = entry.getKey();
            V v = entry.getValue();
            kEncoder.encoder(k, output);
            vEncoder.encoder(v, output);
        }
    }

    @Override
    public Map<K, V> decoder(DataInput input) throws IOException
    {
        final int size = input.readInt();
        Map<K, V> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            K key = kEncoder.decoder(input);
            V value = vEncoder.decoder(input);
            map.put(key, value);
        }
        return map;
    }
}
