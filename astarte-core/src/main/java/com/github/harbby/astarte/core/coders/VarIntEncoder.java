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

import com.github.harbby.astarte.core.api.function.Comparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VarIntEncoder
        implements Encoder<Integer>
{
    /**
     * value:    1 | 2  3  4  5  6  7  8  9  10
     * mapping: -1 | 1 -2  2 -3  3 -4  4 -5  5
     */
    private final boolean optimizeNegativeNumber;
    private final byte[] buffer = new byte[5];

    public VarIntEncoder(boolean optimizeNegativeNumber)
    {
        this.optimizeNegativeNumber = optimizeNegativeNumber;
    }

    public VarIntEncoder()
    {
        this(false);
    }

    @Override
    public void encoder(Integer value, DataOutput output)
            throws IOException
    {
        int v = value;
        if (optimizeNegativeNumber) {
            //v = v >=0 ? value << 1 : (~value) + 1;
            v = (v << 1) ^ (v >> 31);
        }
        buffer[0] = (byte) (v & 0x7F);
        v = v >>> 7;
        if (v == 0) {
            output.write(buffer, 0, 1);
            return;
        }
        buffer[0] |= 0x80;
        buffer[1] = (byte) (v & 0x7F);
        v = v >>> 7;

        if (v == 0) {
            output.write(buffer, 0, 2);
            return;
        }
        buffer[1] |= 0x80;
        buffer[2] = (byte) (v & 0x7F);
        v = v >>> 7;

        if (v == 0) {
            output.write(buffer, 0, 3);
            return;
        }
        buffer[2] |= 0x80;
        buffer[3] = (byte) (v & 0x7F);
        v = v >>> 7;

        if (v == 0) {
            output.write(buffer, 0, 4);
            return;
        }
        // assert v > 0 && v < 16;
        buffer[3] |= 0x80;
        buffer[4] = (byte) v;
        output.write(buffer, 0, 5);
    }

    @Override
    public Integer decoder(DataInput input)
            throws IOException
    {
        byte b = input.readByte();
        int result = b & 0x7F;
        if (b < 0) {
            b = input.readByte();
            result |= (b & 0x7F) << 7;
            if (b < 0) {
                b = input.readByte();
                result |= (b & 0x7F) << 14;
                if (b < 0) {
                    b = input.readByte();
                    result |= (b & 0x7F) << 21;
                    if (b < 0) {
                        b = input.readByte();
                        //assert b > 0;
                        result |= b << 28;
                    }
                }
            }
        }
        if (optimizeNegativeNumber) {
            return (result >>> 1) ^ -(result & 1);
        }
        else {
            return result;
        }
    }

    @Override
    public Comparator<Integer> comparator()
    {
        return Integer::compare;
    }
}
