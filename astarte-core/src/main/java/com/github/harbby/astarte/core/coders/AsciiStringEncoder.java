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

/**
 * @author ivan
 * @date 2021.02.07 21:58
 * Java String Serialize,chose this for letters and numbers
 */
@SuppressWarnings("checkstyle:RegexpMultiline")
public class AsciiStringEncoder
        implements Encoder<String>
{
    @Override
    public void encoder(String value, DataOutput output)
            throws IOException
    {
        if (value == null) {
            output.writeInt(-1);
            return;
        }
        output.writeInt(value.length());
        output.writeBytes(value);
    }

    @Override
    public String decoder(DataInput input)
            throws IOException
    {
        final int length = input.readInt();
        if (length == -1) {
            return null;
        }
        if (length == 0) {
            return "";
        }
        final char[] data = new char[length];
        for (int i = 0; i < length; i++) {
            byte c = input.readByte();
            data[i] = (char) c;
        }
        return new String(data);
    }

    @Override
    public Comparator<String> comparator()
    {
        return String::compareTo;
    }
}
