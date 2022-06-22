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

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class VarIntEncoderTest
{
    @Test
    public void encoder()
            throws IOException
    {
        String a1 = "abc";
        String a2 = "a齐1";
        a1.charAt(0);
        byte[] arr = a2.getBytes(StandardCharsets.UTF_8);
        String a3 = new String(arr, StandardCharsets.UTF_8);

        EncoderChecker<Integer> checker = new EncoderChecker<>(new VarIntEncoder(false));
        byte[] bytes = checker.encoder(42354);  //42354
        Assert.assertArrayEquals(bytes, new byte[] {-14, -54, 2});
    }

    @Test
    public void decoder()
            throws IOException
    {
        EncoderChecker<Integer> checker = new EncoderChecker<>(new VarIntEncoder(false));
        int value = checker.decoder(new byte[] {-14, -54, 2});
        Assert.assertEquals(value, 42354);
    }

    @Test
    public void test()
            throws IOException
    {
        int value = 1073741824;
        EncoderChecker<Integer> checker = new EncoderChecker<>(new VarIntEncoder(true));
        byte[] bytes = checker.encoder(value);
        int rs = checker.decoder(bytes);
        Assert.assertEquals(rs, value);
    }

    @Ignore
    @Test
    public void bigDataTest()
            throws IOException
    {
        EncoderChecker<Integer> checker1 = new EncoderChecker<>(new VarIntEncoder(true));
        for (int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE; i++) {
            byte[] bytes = checker1.encoder(i);
            int v = checker1.decoder(bytes);
            Assert.assertEquals(v, i);
        }

        EncoderChecker<Integer> checker2 = new EncoderChecker<>(new VarIntEncoder(false));
        for (int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE; i++) {
            byte[] bytes = checker2.encoder(i);
            int v = checker2.decoder(bytes);
            Assert.assertEquals(v, i);
        }
    }
}
