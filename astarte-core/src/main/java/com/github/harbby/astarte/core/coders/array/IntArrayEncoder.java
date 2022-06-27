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
import com.github.harbby.astarte.core.coders.io.DataInputView;
import com.github.harbby.astarte.core.coders.io.DataOutputView;

public class IntArrayEncoder
        implements Encoder<int[]>
{
    private static final int[] zeroIntArr = new int[0];

    @Override
    public void encoder(int[] values, DataOutputView output)
    {
        if (values == null) {
            output.writeVarInt(0, false);
            return;
        }
        output.writeVarInt(values.length + 1, false);
        for (int e : values) {
            output.writeInt(e);
        }
    }

    @Override
    public int[] decoder(DataInputView input)
    {
        int len = input.readVarInt(false) - 1;
        if (len == -1) {
            return null;
        }
        if (len == 0) {
            return zeroIntArr;
        }
        int[] values = new int[len];
        for (int i = 0; i < len; i++) {
            values[i] = input.readInt();
        }
        return values;
    }
}
