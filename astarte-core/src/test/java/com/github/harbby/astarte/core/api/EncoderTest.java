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
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

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
}