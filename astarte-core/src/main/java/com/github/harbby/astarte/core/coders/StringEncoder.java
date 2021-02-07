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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author ivan
 * @date 2021.02.07 21:58
 * Java String Serialize
 */
public class StringEncoder implements Encoder<String> {
    //todo 有缺陷，如果字符串长度很大，会有问题
    @Override
    public void encoder(String value, DataOutput output) throws IOException {
        if (value != null) {
            final int length = value.length();
            output.writeInt(length);
            for (int i = 0; i < length; i++) {
                char c = value.charAt(i);
                output.write(c);
            }
        } else {
            output.writeInt(0);
        }


    }

    @Override
    public String decoder(DataInput input) throws IOException {
        final int length = input.readInt();
        if (length == 0) {
            return null;
        }
        final char[] data = new char[length];
        for (int i = 0; i < length; i++) {
            int c = input.readUnsignedByte();
            data[i] = (char) c;
        }
        return new String(data, 0, length);
    }
}
