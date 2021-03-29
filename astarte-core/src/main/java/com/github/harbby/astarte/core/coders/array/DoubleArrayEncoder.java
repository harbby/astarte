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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author ivan
 * @date 2021.02.04 22:52:00
 * double array Serialize
 */
public class DoubleArrayEncoder
        implements Encoder<double[]>
{
    private static final double[] zeroDoubleArr = new double[0];

    @Override
    public void encoder(double[] value, DataOutput output)
            throws IOException
    {
        if (value == null) {
            output.writeInt(-1);
            return;
        }
        output.writeInt(value.length);
        for (double e : value) {
            output.writeDouble(e);
        }
    }

    @Override
    public double[] decoder(DataInput input)
            throws IOException
    {
        final int len = input.readInt();
        if (len == -1) {
            return null;
        }
        if (len == 0) {
            return zeroDoubleArr;
        }
        double[] values = new double[len];
        for (int i = 0; i < len; i++) {
            values[i] = input.readDouble();
        }
        return values;
    }
}
