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

public class VarLongEncoder
        implements Encoder<Long>
{
    private final boolean optimizeNegativeNumber;

    public VarLongEncoder(boolean optimizeNegativeNumber)
    {
        this.optimizeNegativeNumber = optimizeNegativeNumber;
    }

    public VarLongEncoder()
    {
        this(false);
    }

    @Override
    public void encoder(Long value, DataOutputView output)
    {
        output.writeVarLong(value, optimizeNegativeNumber);
    }

    @Override
    public Long decoder(DataInputView input)
    {
        return input.readVarLong(optimizeNegativeNumber);
    }

    @Override
    public Comparator<Long> comparator()
    {
        return Long::compare;
    }
}
