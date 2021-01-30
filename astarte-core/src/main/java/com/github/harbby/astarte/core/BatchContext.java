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
package com.github.harbby.astarte.core;

import com.github.harbby.astarte.core.api.AstarteConf;
import com.github.harbby.astarte.core.api.Constant;
import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.operator.CollectionSource;
import com.github.harbby.astarte.core.operator.KvOperator;
import com.github.harbby.astarte.core.operator.Operator;
import com.github.harbby.astarte.core.operator.TextFileSource;
import com.github.harbby.gadtry.base.Lazys;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import com.github.harbby.gadtry.function.Function1;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public interface BatchContext
{
    public AstarteConf getConf();

    public default <K, V> KvDataSet<K, V> makeKvDataSet(List<Tuple2<K, V>> collection, int parallelism)
    {
        return new KvOperator<>(new CollectionSource<>(this, collection, parallelism));
    }

    public default <V> DataSet<V> makeEmptyDataSet()
    {
        return makeEmptyDataSet(1);
    }

    public default <V> DataSet<V> makeEmptyDataSet(int parallelism)
    {
        return new CollectionSource<>(this, Collections.emptyList(), parallelism);
    }

    public default <K, V> KvDataSet<K, V> makeKvDataSet(List<Tuple2<K, V>> collection)
    {
        return new KvOperator<>(new CollectionSource<>(this, collection, 1));
    }

    public default <E> DataSet<E> makeDataSet(List<E> collection)
    {
        return new CollectionSource<>(this, collection, 1);
    }

    public default <E> DataSet<E> makeDataSet(List<E> collection, int parallelism)
    {
        return new CollectionSource<>(this, collection, parallelism);
    }

    public default <E> DataSet<E> makeDataSet(E... e)
    {
        return makeDataSet(Arrays.asList(e), 1);
    }

    public default <E> DataSet<E> makeDataSet(E[] e, int parallelism)
    {
        return makeDataSet(Arrays.asList(e), parallelism);
    }

    public default DataSet<String> textFile(String dirPath)
    {
        return new TextFileSource(this, dirPath);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private static final Function1<AstarteConf, BatchContext> context = Lazys.goLazy(BatchContextImpl::new);

        private final AstarteConf conf = new AstarteConf();

        public Builder local(int parallelism)
        {
            conf.put(Constant.RUNNING_MODE, String.format("local[%s]", parallelism));
            return this;
        }

        public Builder cluster(int vcores, int executorNum)
        {
            conf.put(Constant.RUNNING_MODE, String.format("cluster[%s,%s]", vcores, executorNum));
            return this;
        }

        public Builder conf(AstarteConf conf)
        {
            this.conf.addConf(conf);
            return this;
        }

        public BatchContext getOrCreate()
        {
            return context.apply(conf);
        }
    }

    public <E, R> List<R> runJob(Operator<E> dataSet, Mapper<Iterator<E>, R> action);
}
