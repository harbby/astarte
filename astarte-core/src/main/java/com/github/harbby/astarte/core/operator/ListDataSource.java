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
import com.github.harbby.gadtry.base.Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ListDataSource<E>
        implements DataSetSource<E>
{
    private final transient List<E> data;

    public ListDataSource(List<E> data)
    {
        this.data = data;
    }

    private static Split[] slice(List<?> data, int parallelism)
    {
        long length = data.size();
        List<Split> list = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            int start = (int) ((i * length) / parallelism);
            int end = (int) (((i + 1) * length) / parallelism);
            if (end > start) {
                list.add(new ParallelCollectionSplit<>(data.subList(start, end).toArray()));
            }
        }
        return list.toArray(new Split[0]);
    }

    @Override
    public Split[] trySplit(int tryParallelism)
    {
        return slice(data, tryParallelism);
    }

    @Override
    public Iterator<E> phyPlan(Split split)
    {
        @SuppressWarnings("unchecked")
        ParallelCollectionSplit<E> dataSplit = (ParallelCollectionSplit<E>) split;
        return Iterators.of(dataSplit.data);
    }

    @Override
    public void pushModePhyPlan(Collector<E> collector, Split split)
    {
        @SuppressWarnings("unchecked")
        ParallelCollectionSplit<E> dataSplit = (ParallelCollectionSplit<E>) split;
        for (E e : dataSplit.data) {
            collector.collect(e);
        }
    }

    @Override
    public void close()
    {
    }

    private static class ParallelCollectionSplit<R>
            implements Split
    {
        private final R[] data;

        public ParallelCollectionSplit(R[] data)
        {
            this.data = data;
        }
    }
}
