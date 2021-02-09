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

import com.github.harbby.gadtry.base.Lazys;
import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.base.Throwables;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Encoder factory
 */
public final class Encoders
{
    protected static final Logger logger = LoggerFactory.getLogger(Encoders.class);

    private Encoders() {}

    private static final Encoder<Long> longEncoder = new LongEncoder();
    private static final Encoder<long[]> longArrEncoder = new LongArrayEncoder();
    private static final long[] zeroLongArr = new long[0];

    private static final Encoder<Integer> intEncoder = new IntEncoder();
    private static final Encoder<int[]> intArrEncoder = new IntArrayEncoder();
    private static final int[] zeroIntArr = new int[0];

    private static final Encoder<Double> doubleEncoder = new DoubleEncoder();
    private static final Encoder<String> stringEncoder = new StringEncoder();

    private static final Supplier<Encoder<?>> javaEncoder = Lazys.goLazy(() -> {
        logger.warn("Don't use java serialize encoder");
        return new DefaultEncoder<>();
    });

    @SuppressWarnings("unchecked")
    public static <E extends Serializable> Encoder<E> javaEncoder()
    {
        return (Encoder<E>) javaEncoder.get();
    }

    private static class DefaultEncoder<E extends Serializable>
            implements Encoder<E>
    {
        @Override
        public void encoder(E value, DataOutput output)
                throws IOException
        {
            byte[] bytes = Serializables.serialize(value);
            output.writeInt(bytes.length);
            output.write(bytes);
        }

        @Override
        public E decoder(DataInput input)
                throws IOException
        {
            byte[] bytes = new byte[input.readInt()];
            input.readFully(bytes);
            try {
                return Serializables.byteToObject(bytes);
            }
            catch (ClassNotFoundException e) {
                throw Throwables.throwsThrowable(e);
            }
        }
    }

    public static <K, V> Encoder<Tuple2<K, V>> tuple2(Encoder<K> kEncoder, Encoder<V> vEncoder)
    {
        requireNonNull(kEncoder, "key Encoder is null");
        requireNonNull(vEncoder, "value Encoder is null");
        return new Tuple2Encoder<>(kEncoder, vEncoder);
    }

    public static Encoder<String> jString()
    {
        return stringEncoder;
    }

    public static Encoder<Long> jLong()
    {
        return longEncoder;
    }

    public static Encoder<long[]> jLongArray()
    {
        return longArrEncoder;
    }

    public static Encoder<Integer> jInt()
    {
        return intEncoder;
    }

    public static Encoder<int[]> jIntArray()
    {
        return intArrEncoder;
    }

    public static Encoder<Double> jDouble()
    {
        return doubleEncoder;
    }

    private static class IntEncoder
            implements Encoder<Integer>
    {
        @Override
        public void encoder(Integer value, DataOutput output)
                throws IOException
        {
            output.writeInt(value);
        }

        @Override
        public Integer decoder(DataInput input)
                throws IOException
        {
            return input.readInt();
        }
    }

    private static class IntArrayEncoder
            implements Encoder<int[]>
    {
        @Override
        public void encoder(int[] values, DataOutput output)
                throws IOException
        {
            output.writeInt(values.length);
            for (int e : values) {
                output.writeInt(e);
            }
        }

        @Override
        public int[] decoder(DataInput input)
                throws IOException
        {
            int len = input.readInt();
            if (len == 0) {
                return zeroIntArr;
            }
            int[] values = new int[len];
            for (int i = 0; i < len; i++) {
                values[i] = input.readInt();
            }
            return values;
        }
    }

    private static class LongEncoder
            implements Encoder<Long>
    {
        @Override
        public void encoder(Long value, DataOutput output)
                throws IOException
        {
            output.writeLong(value);
        }

        @Override
        public Long decoder(DataInput input)
                throws IOException
        {
            return input.readLong();
        }
    }

    private static class DoubleEncoder
            implements Encoder<Double>
    {
        @Override
        public void encoder(Double value, DataOutput output)
                throws IOException
        {
            output.writeDouble(value);
        }

        @Override
        public Double decoder(DataInput input)
                throws IOException
        {
            return input.readDouble();
        }
    }

    private static class LongArrayEncoder
            implements Encoder<long[]>
    {
        @Override
        public void encoder(long[] values, DataOutput output)
                throws IOException
        {
            output.writeInt(values.length);
            for (long e : values) {
                output.writeLong(e);
            }
        }

        @Override
        public long[] decoder(DataInput input)
                throws IOException
        {
            int len = input.readInt();
            if (len == 0) {
                return zeroLongArr;
            }
            long[] values = new long[len];
            for (int i = 0; i < len; i++) {
                values[i] = input.readLong();
            }
            return values;
        }
    }

    private static class ArrayEncoder<E>
            implements Encoder<E[]>
    {
        private final Encoder<E> encoder;
        private final Class<E> classTag;

        private ArrayEncoder(Encoder<E> encoder, Class<E> classTag)
        {
            this.encoder = encoder;
            this.classTag = classTag;
        }

        @Override
        public void encoder(E[] values, DataOutput output)
                throws IOException
        {
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
            @SuppressWarnings("unchecked")
            E[] values = (E[]) java.lang.reflect.Array.newInstance(classTag, len);
            for (int i = 0; i < len; i++) {
                values[i] = encoder.decoder(input);
            }
            return values;
        }
    }

    public static class Tuple2Encoder<K, V>
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

        private Tuple2Encoder(Encoder<K> kEncoder, Encoder<V> vEncoder)
        {
            this.kEncoder = kEncoder;
            this.vEncoder = vEncoder;
        }

        @Override
        public void encoder(Tuple2<K, V> value, DataOutput output)
                throws IOException
        {
            kEncoder.encoder(value.f1, output);
            vEncoder.encoder(value.f2, output);
        }

        @Override
        public Tuple2<K, V> decoder(DataInput input)
                throws IOException
        {
            return new Tuple2<>(kEncoder.decoder(input), vEncoder.decoder(input));
        }
    }
}
