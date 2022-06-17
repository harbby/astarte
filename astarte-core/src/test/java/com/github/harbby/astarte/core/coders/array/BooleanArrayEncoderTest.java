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

import com.github.harbby.astarte.core.coders.EncoderChecker;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class BooleanArrayEncoderTest
{
    private final EncoderChecker<boolean[]> checker = new EncoderChecker<>(new BooleanArrayEncoder());

    @Test
    public void test1()
    {
        boolean[] booleans = new boolean[] {true, false, false, false, true, true, true, false, true};
        byte[] bytes = new byte[(booleans.length + 7) >> 3];
        BooleanArrayEncoder.zip(booleans, 0, bytes, 0, booleans.length);
        Assert.assertArrayEquals(bytes, new byte[] {-114, -128});
        //unzip
        boolean[] rs = new boolean[booleans.length];
        BooleanArrayEncoder.unzip(bytes, 0, rs, 0, rs.length);
        Assert.assertArrayEquals(booleans, rs);
    }

    @Test
    public void test2()
            throws IOException
    {
        boolean[] value = new boolean[] {true, false, false, false, true, true, true, false, true};
        byte[] bytes = checker.encoder(value);
        Assert.assertArrayEquals(bytes, new byte[] {9, -114, -128});
        boolean[] rs = checker.decoder(bytes);
        Assert.assertArrayEquals(value, rs);
    }
}
