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
package com.github.harbby.astarte.core.operator;

import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.codegen.CalcOperator;
import com.github.harbby.gadtry.base.Iterators;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

public class MapOperator<I, O>
        extends CalcOperator<I, O>
{
    private final Mapper<I, O> mapper;

    public MapOperator(Operator<I> dataSet,
            Mapper<I, O> mapper,
            boolean holdPartitioner)
    {
        super(dataSet, holdPartitioner);
        this.mapper = requireNonNull(mapper, "mapper is null");
    }

    @Override
    protected Iterator<O> doCompute(Iterator<I> iterator)
    {
        return Iterators.map(iterator, mapper::map);
    }

    @Override
    public Object getOperator()
    {
        return mapper;
    }
}
