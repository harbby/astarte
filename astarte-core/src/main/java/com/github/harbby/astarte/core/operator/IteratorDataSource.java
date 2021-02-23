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

import com.github.harbby.astarte.core.Utils;
import com.github.harbby.astarte.core.api.Collector;
import com.github.harbby.astarte.core.api.DataSetSource;
import com.github.harbby.astarte.core.api.Split;

import java.io.Serializable;
import java.util.Iterator;

public class IteratorDataSource<E>
        implements DataSetSource<E>
{
    private final Iterator<E> iterator;
    private volatile boolean stop = false;

    public IteratorDataSource(Iterator<E> source)
    {
        this.iterator = (Iterator<E>) Utils.clear((Serializable) source);
    }

    @Override
    public Split[] trySplit(int tryParallelism)
    {
        return new Split[tryParallelism];
    }

    @Override
    public Iterator<E> phyPlan(Split split)
    {
        return iterator;
    }

    @Override
    public void pushModePhyPlan(Collector<E> collector, Split split)
    {
        while (!stop && iterator.hasNext()) {
            collector.collect(iterator.next());
        }
    }

    @Override
    public void close()
    {
        this.stop = true;
    }
}
