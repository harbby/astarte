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

import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.coders.EncoderChecker;
import com.github.harbby.astarte.core.coders.io.BoolArrayZipUtil;
import com.github.harbby.astarte.core.coders.io.DataInputView;
import com.github.harbby.astarte.core.coders.io.DataOutputView;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class BooleanArrayEncoderTest
{
    private final EncoderChecker<boolean[]> checker = new EncoderChecker<>(new Encoder<boolean[]>() {
        @Override
        public void encoder(boolean[] value, DataOutputView output)
        {
            output.writeVarInt(value.length, false);
            output.writeBoolArray(value);
        }

        @Override
        public boolean[] decoder(DataInputView input)
        {
            int len = input.readVarInt(false);
            boolean[] booleans = new boolean[len];
            input.readBoolArray(booleans, 0, len);
            return booleans;
        }
    });

    @Test
    public void test1()
    {
        boolean[] booleans = new boolean[] {true, false, false, false, true, true, true, false, true};
        byte[] bytes = new byte[(booleans.length + 7) >> 3];
        BoolArrayZipUtil.zip(booleans, 0, bytes, 0, booleans.length);
        Assert.assertArrayEquals(bytes, new byte[] {-114, -128});
        //unzip
        boolean[] rs = new boolean[booleans.length];
        BoolArrayZipUtil.unzip(bytes, 0, rs, 0, rs.length);
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
