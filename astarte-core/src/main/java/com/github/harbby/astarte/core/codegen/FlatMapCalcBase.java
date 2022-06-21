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
package com.github.harbby.astarte.core.codegen;

import com.github.harbby.astarte.core.operator.CalcOperator;

import java.util.Iterator;
import java.util.List;

public abstract class FlatMapCalcBase<O>
        implements Iterator<O>
{
    public FlatMapCalcBase(List<CalcOperator<?, ?>> operators)
    {
    }

    public abstract Iterator<O> begin(Iterator<?> childIterator);
}
