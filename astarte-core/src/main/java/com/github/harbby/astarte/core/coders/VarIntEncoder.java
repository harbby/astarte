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
import com.github.harbby.astarte.core.coders.io.DataInputView;
import com.github.harbby.astarte.core.coders.io.DataOutputView;

public class VarIntEncoder
        implements Encoder<Integer>
{
    /**
     * value:    1 | 2  3  4  5  6  7  8  9  10
     * mapping: -1 | 1 -2  2 -3  3 -4  4 -5  5
     */
    private final boolean optimizeNegativeNumber;

    public VarIntEncoder(boolean optimizeNegativeNumber)
    {
        this.optimizeNegativeNumber = optimizeNegativeNumber;
    }

    public VarIntEncoder()
    {
        this(false);
    }

    @Override
    public void encoder(Integer value, DataOutputView output)
    {
        output.writeVarInt(value, optimizeNegativeNumber);
    }

    @Override
    public Integer decoder(DataInputView input)
    {
        return input.readVarInt(optimizeNegativeNumber);
    }

    @Override
    public Comparator<Integer> comparator()
    {
        return Integer::compare;
    }
}
