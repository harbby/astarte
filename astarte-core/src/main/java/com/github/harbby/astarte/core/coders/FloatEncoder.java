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
 * @date 2021.02.09 10:01:00
 * float Serialize
 */
public class FloatEncoder
        implements Encoder<Float>
{
    protected FloatEncoder() {}

    @Override
    public void encoder(Float value, DataOutput output) throws IOException
    {
        output.writeFloat(value);
    }

    @Override
    public Float decoder(DataInput input) throws IOException
    {
        return input.readFloat();
    }

    @Override
    public Comparator<Float> comparator()
    {
        return Float::compare;
    }
}