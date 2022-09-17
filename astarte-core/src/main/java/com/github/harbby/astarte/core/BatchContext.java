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
import com.github.harbby.astarte.core.api.DataSet;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.coders.Encoders;
import com.github.harbby.astarte.core.operator.DataSourceOperator;
import com.github.harbby.astarte.core.operator.KvOperator;
import com.github.harbby.astarte.core.operator.ListDataSource;
import com.github.harbby.astarte.core.operator.Operator;
import com.github.harbby.astarte.core.operator.TextFileSource;
import com.github.harbby.astarte.core.runtime.ClusterScheduler;
import com.github.harbby.astarte.core.runtime.ExecutorManager;
import com.github.harbby.astarte.core.runtime.ForkVmExecutorManager;
import com.github.harbby.astarte.core.runtime.LocalJobScheduler;
import com.github.harbby.astarte.core.runtime.LocalNettyExecutorManager;
import com.github.harbby.gadtry.base.Lazys;
import com.github.harbby.gadtry.collection.ImmutableList;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

public interface BatchContext
{
    public AstarteConf getConf();

    public void stop();

    public default <K, V> KvDataSet<K, V> makeKvDataSet(List<Tuple2<K, V>> collection, int parallelism)
    {
        checkState(parallelism > 0, "parallelism > 0");
        return new KvOperator<>(new DataSourceOperator<>(this,
                new ListDataSource<>(collection),
                parallelism,
                Encoders.tuple2(Encoders.javaEncoder(), Encoders.javaEncoder())));
    }

    public default <K, V> KvDataSet<K, V> makeKvDataSet(List<Tuple2<K, V>> collection)
    {
        return makeKvDataSet(collection, 1);
    }

    public default <V> DataSet<V> makeEmptyDataSet()
    {
        return makeDataSet(Collections.emptyList());
    }

    public default <E> DataSet<E> makeDataSet(List<E> collection)
    {
        return this.makeDataSet(collection, 1);
    }

    public default <E> DataSet<E> makeDataSet(List<E> collection, Encoder<E> encoder)
    {
        return this.makeDataSet(collection, 1, encoder);
    }

    public default <E> DataSet<E> makeDataSet(List<E> collection, int parallelism)
    {
        return this.makeDataSet(collection, parallelism, Encoders.javaEncoder());
    }

    public default <E> DataSet<E> makeDataSet(List<E> collection, int parallelism, Encoder<E> encoder)
    {
        requireNonNull(collection, "collection is null");
        checkState(parallelism > 0, "parallelism > 0");
        return new DataSourceOperator<>(this, new ListDataSource<>(collection), parallelism, encoder);
    }

    public default <E> DataSet<E> makeDataSet(E[] array)
    {
        return makeDataSet(array, 1);
    }

    public default <E> DataSet<E> makeDataSet(E[] array, int parallelism)
    {
        requireNonNull(array, "array is null");
        @SuppressWarnings("unchecked")
        Encoder<E> encoder = Encoders.createPrimitiveEncoder((Class<E>) array.getClass().getComponentType());
        return makeDataSet(ImmutableList.copy(array), parallelism, encoder);
    }

    public default DataSet<String> textFile(String path)
    {
        requireNonNull(path, "path is null");
        return new DataSourceOperator<>(this, new TextFileSource(URI.create(path)), -1, Encoders.string());
    }

    public default DataSet<String> textFile(String path, int parallelism)
    {
        checkState(parallelism > 0, "parallelism > 0");
        requireNonNull(path, "path is null");
        return new DataSourceOperator<>(this, new TextFileSource(URI.create(path)), parallelism, Encoders.string());
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private static final Function<AstarteConf, BatchContext> context = Lazys.of(BatchContextImpl::new);
        private final AstarteConf conf = new AstarteConf();
        private JobScheduler.Factory factory;
        private ExecutorManager.Factory emFactory;

        /**
         * 独特的local调度与运行模式。不经过网络接口协议，比netLocal更加轻巧简单
         * 适合进行算子与执行计划研究
         *
         * @param parallelism 并行度
         * @return Builder
         */
        public Builder local(int parallelism)
        {
            this.factory = f -> new LocalJobScheduler(parallelism);
            return this;
        }

        /**
         * 通过local网络提供调度和数据shuffle的模式。类似与spark和flink的local模式。
         * 适合进行网络通信层面的研究
         *
         * @param parallelism 并行度
         * @return Builder
         */
        public Builder netLocal(int parallelism)
        {
            this.factory = f -> new ClusterScheduler(f, parallelism, 1);
            this.emFactory = LocalNettyExecutorManager::new;
            return this;
        }

        /**
         * 独特的伪分布式运行模式。运行时将所有Executor以Fork子进程方式启动。和真实集群运行一样的调度及shuffle数据传输模式
         * 适合进行快速分布式调度模块研究,该模式验证所有功能在其他物理集群的功能可行性
         *
         * @param vcores      单个Executor并行度
         * @param executorNum Executor数量
         * @return Builder
         */
        public Builder localCluster(int vcores, int executorNum)
        {
            this.factory = f -> new ClusterScheduler(f, vcores, executorNum);
            this.emFactory = ForkVmExecutorManager::new;
            return this;
        }

        public Builder conf(AstarteConf conf)
        {
            this.conf.addConf(conf);
            return this;
        }

        public BatchContext getOrCreate()
        {
            if (factory != null) {
                JobScheduler.setFactory(factory);
            }
            if (emFactory != null) {
                ExecutorManager.setFactory(emFactory);
            }
            return context.apply(conf);
        }
    }

    public <E, R> List<R> runJob(Operator<E> dataSet, Mapper<Iterator<E>, R> action);
}
