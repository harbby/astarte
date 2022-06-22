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

import com.github.harbby.astarte.core.api.Tuple2;
import com.github.harbby.astarte.core.api.function.Comparator;
import com.github.harbby.astarte.core.coders.io.DataInputView;
import com.github.harbby.astarte.core.coders.io.DataOutputView;

import static java.util.Objects.requireNonNull;

public interface Tuple2Encoder<K, V>
        extends Encoder<Tuple2<K, V>>
{
    public Encoder<K> getKeyEncoder();

    public Encoder<V> getValueEncoder();

    public static class Tuple2KVEncoder<K, V>
            implements Tuple2Encoder<K, V>
    {
        private final Encoder<K> kEncoder;
        private final Encoder<V> vEncoder;

        public Tuple2KVEncoder(Encoder<K> kEncoder, Encoder<V> vEncoder)
        {
            this.kEncoder = requireNonNull(kEncoder, "kEncoder is null");
            this.vEncoder = requireNonNull(vEncoder, "vEncoder is null");
        }

        @Override
        public Encoder<K> getKeyEncoder()
        {
            return kEncoder;
        }

        @Override
        public Encoder<V> getValueEncoder()
        {
            return vEncoder;
        }

        @Override
        public void encoder(Tuple2<K, V> value, DataOutputView output)
        {
            requireNonNull(value, "Tuple2 value is null");
            kEncoder.encoder(value.key(), output);
            vEncoder.encoder(value.value(), output);
        }

        @Override
        public Tuple2<K, V> decoder(DataInputView input)
        {
            return Tuple2.of(kEncoder.decoder(input), vEncoder.decoder(input));
        }

        @Override
        public Comparator<Tuple2<K, V>> comparator()
        {
            return (kv1, kv2) -> {
                int than = kEncoder.comparator().compare(kv1.key(), kv2.key());
                if (than != 0) {
                    return than;
                }
                return vEncoder.comparator().compare(kv1.value(), kv2.value());
            };
        }
    }

    public static class Tuple2OnlyKeyEncoder<K>
            implements Tuple2Encoder<K, Void>
    {
        private final Encoder<K> kEncoder;

        public Tuple2OnlyKeyEncoder(Encoder<K> kEncoder)
        {
            this.kEncoder = requireNonNull(kEncoder, "kEncoder is null");
        }

        @Override
        public Encoder<K> getKeyEncoder()
        {
            return kEncoder;
        }

        @Override
        public Encoder<Void> getValueEncoder()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void encoder(Tuple2<K, Void> value, DataOutputView output)
        {
            kEncoder.encoder(value.key(), output);
        }

        @Override
        public Tuple2<K, Void> decoder(DataInputView input)
        {
            return Tuple2.of(kEncoder.decoder(input), null);
        }

        @Override
        public Comparator<Tuple2<K, Void>> comparator()
        {
            return (kv1, kv2) -> kEncoder.comparator().compare(kv1.key(), kv2.key());
        }
    }
}
