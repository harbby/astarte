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
package com.github.harbby.astarte.core;

import com.github.harbby.astarte.core.api.BaseRow;

import java.util.Objects;

public interface Tuple2Bak<K, V>
        extends BaseRow
{
    public K key();

    public V value();

    public void setKey(K k);

    public void setValue(V v);

    public static <K, V> Tuple2Bak<K, V> of(K k, V v)
    {
        return new JTuple2Bak<>(k, v);
    }

    public static class JTuple2Bak<K, V>
            implements Tuple2Bak<K, V>
    {
        public K k;
        public V v;

        public JTuple2Bak(K k, V v)
        {
            this.k = k;
            this.v = v;
        }

        @Override
        public void setKey(K k)
        {
            this.k = k;
        }

        @Override
        public void setValue(V v)
        {
            this.v = v;
        }

        @Override
        public K key()
        {
            return k;
        }

        @Override
        public V value()
        {
            return v;
        }

        @Override
        public int size()
        {
            return 2;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T getField(int index)
        {
            switch (index) {
                case 0:
                    return (T) k;
                case 1:
                    return (T) v;
                default:
                    throw new IndexOutOfBoundsException();
            }
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(k, v);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }

            JTuple2Bak<?, ?> other = (JTuple2Bak<?, ?>) obj;
            return Objects.equals(this.k, other.k) &&
                    Objects.equals(this.v, other.v);
        }

        @Override
        public JTuple2Bak<K, V> copy()
        {
            return new JTuple2Bak<>(k, v);
        }

        @Override
        public String toString()
        {
            return String.format("(%s, %s)", k, v);
        }
    }
}
