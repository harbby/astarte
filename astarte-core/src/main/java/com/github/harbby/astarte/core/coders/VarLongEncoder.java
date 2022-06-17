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

public class VarLongEncoder
        implements Encoder<Long>
{
    private final boolean optimizeNegativeNumber;
    private final byte[] buffer = new byte[9];

    public VarLongEncoder(boolean optimizeNegativeNumber)
    {
        this.optimizeNegativeNumber = optimizeNegativeNumber;
    }

    public VarLongEncoder()
    {
        this(false);
    }

    @Override
    public void encoder(Long value, DataOutput output)
            throws IOException
    {
        long v = value;
        if (optimizeNegativeNumber) {
            //v = v >=0 ? value << 1 : (~value) + 1;
            v = (v << 1) ^ (v >> 63);
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
        buffer[3] |= 0x80;
        buffer[4] = (byte) (v & 0x7F);
        v = v >>> 7;

        if (v == 0) {
            output.write(buffer, 0, 5);
            return;
        }
        buffer[4] |= 0x80;
        buffer[5] = (byte) (v & 0x7F);
        v = v >>> 7;

        if (v == 0) {
            output.write(buffer, 0, 6);
            return;
        }
        buffer[5] |= 0x80;
        buffer[6] = (byte) (v & 0x7F);
        v = v >>> 7;

        if (v == 0) {
            output.write(buffer, 0, 7);
            return;
        }
        buffer[6] |= 0x80;
        buffer[7] = (byte) (v & 0x7F);
        v = v >>> 7;

        if (v == 0) {
            output.write(buffer, 0, 8);
            return;
        }
        buffer[7] |= 0x80;
        buffer[8] = (byte) v;
        output.write(buffer, 0, 9);
    }

    @Override
    public Long decoder(DataInput input)
            throws IOException
    {
        byte b = input.readByte();
        long result = b & 0x7F;
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
                        result |= (long) (b & 0x7F) << 28;
                        if (b < 0) {
                            b = input.readByte();
                            result |= (long) (b & 0x7F) << 35;
                            if (b < 0) {
                                b = input.readByte();
                                result |= (long) (b & 0x7F) << 42;
                                if (b < 0) {
                                    b = input.readByte();
                                    result |= (long) (b & 0x7F) << 49;
                                    if (b < 0) {
                                        b = input.readByte();
                                        result |= (long) b << 56;
                                    }
                                }
                            }
                        }
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
    public Comparator<Long> comparator()
    {
        return Long::compare;
    }
}
