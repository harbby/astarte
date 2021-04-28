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
 * string array Serialize
 */
public class StringArrayEncoder
        implements Encoder<String[]>
{
    private static final String[] zeroStringArr = new String[0];

    @Override
    public void encoder(String[] value, DataOutput output)
            throws IOException
    {
        output.writeInt(value.length);
        for (String e : value) {
            output.writeUTF(e);
        }
    }

    @Override
    public String[] decoder(DataInput input)
            throws IOException
    {
        final int len = input.readInt();
        if (len == 0) {
            return zeroStringArr;
        }
        String[] values = new String[len];
        for (int i = 0; i < len; i++) {
            values[i] = input.readUTF();
        }
        return values;
    }
}
