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

import com.github.harbby.gadtry.base.Serializables;
import com.github.harbby.gadtry.base.Throwables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class JavaEncoder<E extends Serializable>
        implements Encoder<E>
{
    private static final long serialVersionUID = -7686841423755982202L;

    @Override
    public void encoder(E value, DataOutput output)
            throws IOException
    {
        byte[] bytes = Serializables.serialize(value);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    @Override
    public E decoder(DataInput input)
            throws IOException
    {
        byte[] bytes = new byte[input.readInt()];
        input.readFully(bytes);
        try {
            return Serializables.byteToObject(bytes);
        }
        catch (ClassNotFoundException e) {
            throw Throwables.throwsThrowable(e);
        }
    }
}
