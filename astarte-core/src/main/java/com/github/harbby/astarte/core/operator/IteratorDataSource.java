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

import com.github.harbby.astarte.core.api.Collector;
import com.github.harbby.astarte.core.api.DataSetSource;
import com.github.harbby.astarte.core.api.Split;
import com.github.harbby.gadtry.function.Creator;

import java.util.Iterator;

public class IteratorDataSource<E>
        implements DataSetSource<E>
{
    private final Creator<Iterator<E>> source;
    private transient volatile boolean stop = false;

    public IteratorDataSource(Creator<Iterator<E>> source)
    {
        this.source = source;
    }

    @Override
    public Split[] trySplit(int tryParallelism)
    {
        return new Split[tryParallelism];
    }

    @Override
    public Iterator<E> phyPlan(Split split)
    {
        return source.get();
    }

    @Override
    public void pushModePhyPlan(Collector<E> collector, Split split)
    {
        Iterator<E> iterator = source.get();
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
