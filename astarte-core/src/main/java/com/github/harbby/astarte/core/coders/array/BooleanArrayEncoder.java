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
import com.github.harbby.astarte.core.coders.Encoders;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * boolean array Serialize
 */
public class BooleanArrayEncoder
        implements Encoder<boolean[]>
{
    private static final boolean[] EMPTY = new boolean[0];
    private byte[] buffer;
    private static final Encoder<Integer> encoder = Encoders.varInt(false);

    @Override
    public void encoder(boolean[] value, DataOutput output)
            throws IOException
    {
        if (value == null) {
            encoder.encoder(-1, output);
            return;
        }
        else if (value.length == 0) {
            encoder.encoder(0, output);
            return;
        }
        int byteSize = (value.length + 7) >> 3;
        if (buffer == null || buffer.length < byteSize) {
            buffer = new byte[byteSize];
        }
        zip(value, 0, buffer, 0, value.length);

        encoder.encoder(value.length, output);
        output.write(buffer, 0, byteSize);
    }

    @Override
    public boolean[] decoder(DataInput input)
            throws IOException
    {
        final int len = encoder.decoder(input);
        if (len == -1) {
            return null;
        }
        if (len == 0) {
            return EMPTY;
        }
        boolean[] values = new boolean[len];
        int byteSize = (len + 7) >> 3;
        input.readFully(buffer, 0, byteSize);

        unzip(buffer, 0, values, 0, len);
        return values;
    }

    public static void zip(boolean[] src, int srcPos, byte[] dest, int destPos, int dataLength)
    {
        int byteSize = (dataLength + 7) >> 3;
        Arrays.fill(dest, destPos, destPos + byteSize, (byte) 0);
        for (int i = 0; i < dataLength; i++) {
            if (src[i + srcPos]) {
                dest[(i >> 3) + destPos] |= 0x80 >> (i & 7);
            }
        }
    }

    public static void unzip(byte[] src, int srcPos, boolean[] dest, int destPos, int dataLength)
    {
        for (int i = 0; i < dataLength; i++) {
            byte v = src[(i >> 3) + srcPos];
            dest[i + destPos] = (v & (0x80 >> (i & 7))) != 0;
        }
    }
}
