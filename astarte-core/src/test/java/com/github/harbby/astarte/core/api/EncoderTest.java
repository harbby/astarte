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
package com.github.harbby.astarte.core.api;

import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.coders.Encoders;
import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.collection.MutableMap;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import com.github.harbby.gadtry.function.exception.Consumer;
import com.github.harbby.gadtry.function.exception.Function2;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

public class EncoderTest
{
    @Test
    public void javaSerializeTest()
            throws IOException
    {
        Tuple2<Long, Long> tuple2 = Tuple2.of(1L, 2L);
        byte[] bytes = Serializables.serialize(tuple2);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(outputStream);
        Encoder<Tuple2<Long, Long>> tuple2Encoder = Encoders.tuple2(Encoders.jLong(), Encoders.jLong());
        tuple2Encoder.encoder(tuple2, dataOutput);

        Assert.assertEquals(outputStream.toByteArray().length, 16);
        Assert.assertTrue(bytes.length > 16 * 10);
    }

    @Test
    public void Tuple2JavaSerializeThanJavaSerializeTest()
            throws IOException
    {
        Function2<Tuple2<Long, Long>, Encoder<Tuple2<Long, Long>>, byte[], IOException> checker = (tuple2, encoder) -> {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            encoder.encoder(tuple2, new DataOutputStream(outputStream));
            Tuple2<Long, Long> checkObj = encoder.decoder(new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())));
            Assert.assertEquals(tuple2, checkObj);
            return outputStream.toByteArray();
        };
        byte[] bytes = checker.apply(Tuple2.of(1L, 2L), Encoders.javaEncoder());
        byte[] bytes2 = checker.apply(Tuple2.of(1L, 2L), Encoders.tuple2(Encoders.javaEncoder(), Encoders.javaEncoder()));
        Assert.assertTrue(bytes.length > bytes2.length);
    }

    @Test
    public void mapSerializeTest()
            throws IOException
    {
        Encoder<Map<String, String>> mapEncoder = Encoders.mapEncoder(Encoders.asciiString(), Encoders.jCharString());
        Consumer<Map<String, String>, IOException> checker = map -> {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            mapEncoder.encoder(map, new DataOutputStream(outputStream));
            Map<String, String> decoder = mapEncoder.decoder(new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())));
            Assert.assertEquals(decoder, map);
        };
        checker.apply(MutableMap.of(
                "weight", "1",
                "height", "2",
                null, "3",
                "ss", null));
        checker.apply(null);
    }

    @Test
    public void arrayEncoderTest()
            throws IOException
    {
        Encoder<String[]> encoder = Encoders.arrayEncoder(Encoders.asciiString(), String.class);
        Consumer<String[], IOException> checker = array -> {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            encoder.encoder(array, new DataOutputStream(outputStream));
            String[] out = encoder.decoder(new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())));
            Assert.assertArrayEquals(out, array);
        };
        checker.apply(new String[] {"a1", "a2"});
        checker.apply(null);
    }

    @Test
    public void booleanSerializeTest()
            throws IOException
    {
        Encoder<Boolean> booleanEncoder = Encoders.jBoolean();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(outputStream);
        booleanEncoder.encoder(true, dataOutput);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        Boolean decoder = booleanEncoder.decoder(input);

        Assert.assertTrue(decoder == true);
    }

    @Test
    public void stringSerializeTest()
            throws IOException
    {
//        Encoder<String> stringEncoder = Encoders.jByteString();
        Encoder<String> stringEncoder = Encoders.jCharString();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(outputStream);
        stringEncoder.encoder("yes", dataOutput);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        String decoder = stringEncoder.decoder(input);

        Assert.assertTrue(decoder.equals("yes"));
    }

    @Test
    public void byteSerializeTest()
            throws IOException
    {
        Encoder<Byte> byteEncoder = Encoders.jByte();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(outputStream);
        byte a = (byte) 127;
        byteEncoder.encoder(a, dataOutput);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        byte decoder = byteEncoder.decoder(input);

        Assert.assertTrue(decoder == a);
    }

    @Test
    public void charSerializeTest()
            throws IOException
    {
        Encoder<Character> characterEncoder = Encoders.jChar();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(outputStream);
        char a = 'a';
        characterEncoder.encoder(a, dataOutput);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        char decoder = characterEncoder.decoder(input);

        Assert.assertTrue(decoder == a);
    }

    @Test
    public void dateSerializeTest()
            throws IOException
    {
        Encoder<Date> dateEncoder = Encoders.jDate();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(outputStream);
        Date a = null;
//        Date a = new Date();
        dateEncoder.encoder(a, dataOutput);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        Date decoder = dateEncoder.decoder(input);

        Assert.assertTrue(decoder == null);
//        Assert.assertTrue(decoder.getTime() == a.getTime());
    }

    @Test
    public void shortSerializeTest()
            throws IOException
    {
        Encoder<Short> shortEncoder = Encoders.jShort();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(outputStream);
        Short a = (short) 10;
        shortEncoder.encoder(a, dataOutput);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        Short decoder = shortEncoder.decoder(input);

        Assert.assertTrue(decoder == a);
    }

    @Test
    public void floatSerializeTest()
            throws IOException
    {
        Encoder<Float> floatEncoder = Encoders.jFloat();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(outputStream);
        float a = 10.0f;
        floatEncoder.encoder(a, dataOutput);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        float decoder = floatEncoder.decoder(input);

        Assert.assertTrue(decoder == a);
    }

    @Test
    public void sqlDateSerializeTest()
            throws IOException
    {
        Encoder<java.sql.Date> dateEncoder = Encoders.sqlDate();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(outputStream);
        java.sql.Date a = new java.sql.Date(1567865756L);
        dateEncoder.encoder(a, dataOutput);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        java.sql.Date decoder = dateEncoder.decoder(input);

        Assert.assertTrue(decoder.getTime() == a.getTime());
    }

    @Test
    public void sqlTimestampSerializeTest()
            throws IOException
    {
        Encoder<Timestamp> timestampEncoder = Encoders.sqlTimestamp();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(outputStream);
        Timestamp a = new Timestamp(1567865756L);
        timestampEncoder.encoder(a, dataOutput);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInput input = new DataInputStream(inputStream);
        Timestamp decoder = timestampEncoder.decoder(input);

//        Assert.assertTrue(decoder == a);
        Assert.assertTrue(decoder.getTime() == a.getTime());
        Assert.assertTrue(decoder.getNanos() == a.getNanos());
    }
}
