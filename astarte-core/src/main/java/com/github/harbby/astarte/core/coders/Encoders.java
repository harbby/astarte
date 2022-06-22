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

import com.github.harbby.astarte.core.coders.array.AnyArrayEncoder;
import com.github.harbby.astarte.core.coders.array.IntArrayEncoder;
import com.github.harbby.gadtry.base.Lazys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.function.Supplier;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Encoder factory
 */
public final class Encoders
{
    private static final Logger logger = LoggerFactory.getLogger(Encoders.class);

    private Encoders() {}

    private static final Encoder<Long> longEncoder = new LongEncoder();
    private static final Encoder<Boolean> booleanEncoder = new BooleanEncoder();
    private static final Encoder<Byte> byteEncoder = new ByteEncoder();
    private static final Encoder<Character> charEncoder = new CharEncoder();
    private static final Encoder<Short> shortEncoder = new ShortEncoder();
    private static final Encoder<Float> floatEncoder = new FloatEncoder();
    private static final Encoder<java.sql.Date> sqlDateEncoder = new SqlDateEncoder();
    private static final Encoder<Timestamp> timestampEncoder = new SqlTimestampEncoder();

    private static final Encoder<Integer> intEncoder = new IntEncoder();
    private static final Encoder<int[]> intArrEncoder = new IntArrayEncoder();

    private static final Encoder<Double> doubleEncoder = new DoubleEncoder();
    private static final Encoder<String> charStringEncoder = new CharStringEncoder();
    private static final Encoder<String> ASCII_STRING_ENCODER = new AsciiStringEncoder();
    private static final Encoder<String> UTF8_STRING_ENCODER = new UTF8StringEncoder();

    private static final Supplier<Encoder<?>> javaEncoder = Lazys.goLazy(() -> {
        logger.warn("Don't use java serialization encoder");
        return new JavaEncoder<>();
    });

    @SuppressWarnings("unchecked")
    public static <E> Encoder<E> javaEncoder()
    {
        return (Encoder<E>) javaEncoder.get();
    }

    public static <E> Encoder<E> createPrimitiveEncoder(Class<E> aClass)
    {
        requireNonNull(aClass, "aClass is null");
        Encoder<?> encoder;
        checkState(!aClass.isArray());
        if (aClass == String.class) {
            encoder = new UTF8StringEncoder();
        }
        else if (aClass == int.class || aClass == Integer.class) {  //Integer.TYPE
            encoder = jInt();
        }
        else if (aClass == short.class || aClass == Short.class) {
            encoder = jShort();
        }
        else if (aClass == long.class || aClass == Long.class) {
            encoder = jLong();
        }
        else if (aClass == float.class || aClass == Float.class) {
            encoder = jFloat();
        }
        else if (aClass == double.class || aClass == Double.class) {
            encoder = jDouble();
        }
        else if (aClass == byte.class || aClass == Byte.class) {
            encoder = jByte();
        }
        else if (aClass == boolean.class || aClass == Boolean.class) {
            encoder = jBoolean();
        }
        else if (aClass == char.class || aClass == Character.class) {
            encoder = jChar();
        }
        else {
            encoder = javaEncoder();
        }
        return (Encoder<E>) encoder;
    }

    public static <K, V> MapEncoder<K, V> mapEncoder(Encoder<K> kEncoder, Encoder<V> vEncoder)
    {
        requireNonNull(kEncoder, "key Encoder is null");
        requireNonNull(vEncoder, "value Encoder is null");
        return new MapEncoder<>(kEncoder, vEncoder);
    }

    public static <V> AnyArrayEncoder<V> arrayEncoder(Encoder<V> vEncoder, Class<V> aClass)
    {
        requireNonNull(vEncoder, "value Encoder is null");
        return new AnyArrayEncoder<>(vEncoder, aClass);
    }

    public static <K, V> Tuple2Encoder<K, V> tuple2(Encoder<K> kEncoder, Encoder<V> vEncoder)
    {
        requireNonNull(kEncoder, "key Encoder is null");
        requireNonNull(vEncoder, "value Encoder is null");
        return new Tuple2Encoder.Tuple2KVEncoder<>(kEncoder, vEncoder);
    }

    public static <K> Tuple2Encoder<K, Void> tuple2OnlyKey(Encoder<K> kEncoder)
    {
        requireNonNull(kEncoder, "key Encoder is null");
        return new Tuple2Encoder.Tuple2OnlyKeyEncoder<>(kEncoder);
    }

    public static <E> Encoder<Iterator<E>> iteratorEncoder(Encoder<E> eEncoder)
    {
        return new LengthIteratorEncoder<>(eEncoder);
    }

    public static Encoder<String> jCharString()
    {
        return charStringEncoder;
    }

    public static Encoder<String> asciiString()
    {
        return ASCII_STRING_ENCODER;
    }

    public static Encoder<String> UTF8String()
    {
        return UTF8_STRING_ENCODER;
    }

    public static Encoder<Boolean> jBoolean()
    {
        return booleanEncoder;
    }

    public static Encoder<Byte> jByte()
    {
        return byteEncoder;
    }

    public static Encoder<Float> jFloat()
    {
        return floatEncoder;
    }

    public static Encoder<Short> jShort()
    {
        return shortEncoder;
    }

    public static Encoder<java.sql.Date> sqlDate()
    {
        return sqlDateEncoder;
    }

    public static Encoder<Timestamp> sqlTimestamp()
    {
        return timestampEncoder;
    }

    public static Encoder<Character> jChar()
    {
        return charEncoder;
    }

    public static Encoder<Long> jLong()
    {
        return longEncoder;
    }

    public static Encoder<Integer> jInt()
    {
        return intEncoder;
    }

    public static Encoder<Integer> varInt(boolean optimizeNegativeNumber)
    {
        return new VarIntEncoder(optimizeNegativeNumber);
    }

    public static Encoder<Integer> varInt()
    {
        return new VarIntEncoder();
    }

    public static Encoder<Long> varLong()
    {
        return new VarLongEncoder();
    }

    public static Encoder<Long> varLong(boolean optimizeNegativeNumber)
    {
        return new VarLongEncoder(optimizeNegativeNumber);
    }

    public static Encoder<int[]> jIntArray()
    {
        return intArrEncoder;
    }

    public static Encoder<Double> jDouble()
    {
        return doubleEncoder;
    }
}
