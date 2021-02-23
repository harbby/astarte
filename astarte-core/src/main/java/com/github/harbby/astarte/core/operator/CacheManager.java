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

import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.memory.ByteCachedMemory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static java.util.Objects.requireNonNull;

/**
 * cache数据，支持对象和字节存储(支持3种cache模式)
 * cache算子会使得Executor节点拥有状态，调度时应注意幂等
 */
public class CacheManager
{
    private CacheManager() {}

    private static final Logger logger = LoggerFactory.getLogger(CacheManager.class);
    private static final Map<Integer, DataSetCache<?>> cacheMemMap = new ConcurrentHashMap<>();

    public enum CacheMode
    {
        MEM_ONLY,
        MEM_DISK,
        DISK_ONLY; //todo: checkpoint ?
    }

    public abstract static class CacheMemory<E>
    {
        protected boolean isFinal;

        public abstract void freeMemory();

        public void finalCache()
        {
            this.isFinal = true;
        }

        public abstract void append(E record);

        public abstract Iterator<E> prepareIterator();
    }

    private static class DataSetCache<E>
    {
        private final int number;
        private final AtomicInteger releasePartitions = new AtomicInteger();
        private final CacheMemory<E>[] cacheMemories;

        @SuppressWarnings("unchecked")
        private DataSetCache(int dataSetId, int number)
        {
            this.number = number;
            this.cacheMemories = new CacheMemory[number];
        }

        public CacheMemory<E> getCache(int partitionId)
        {
            return cacheMemories[partitionId];
        }

        public void putCache(int partitionId, CacheMemory<E> cacheMemory)
        {
            cacheMemories[partitionId] = cacheMemory;
        }

        public int freePartition(int partitionId)
        {
            requireNonNull(cacheMemories[partitionId], "unknown core error").freeMemory();
            cacheMemories[partitionId] = null;
            return releasePartitions.incrementAndGet();
        }

        public void freeAllPartition()
        {
            for (CacheMemory<?> byteCachedMemory : cacheMemories) {
                requireNonNull(byteCachedMemory, "unknown core error").freeMemory();
            }
        }
    }

    public static void unCacheExec(int dataSetId)
    {
        DataSetCache<?> cachedMemories = cacheMemMap.remove(dataSetId);
        if (cachedMemories != null) {
            cachedMemories.freeAllPartition();
            logger.info("cleared dataSet[{}] cache data", dataSetId);
        }
    }

    public static void unCacheExec(int dataSetId, int partitionId)
    {
        DataSetCache<?> dataSetCache = cacheMemMap.get(dataSetId);
        int released = dataSetCache.freePartition(partitionId);
        logger.info("cleared dataSet[{}] partition[{}] cache data", dataSetId, partitionId);

        if (released >= dataSetCache.number) {
            if (cacheMemMap.remove(dataSetId) != null) {
                logger.info("Dataset[{}] all partition cache data cleared", dataSetId);
            }
        }
    }

    /**
     * Cache数据，使用对象存储
     */
    private static class ObjectCacheMemory<E>
            extends CacheMemory<E>
    {
        private final List<E> data = new LinkedList<>();

        @Override
        public void freeMemory()
        {
            data.clear();
        }

        @Override
        public void append(E record)
        {
            checkState(!isFinal, "don't append record to writeMode");
            data.add(record);
        }

        @Override
        public Iterator<E> prepareIterator()
        {
            checkState(isFinal, "must be in read mode");
            return data.iterator();
        }
    }

    /**
     * todo: fix bugs
     */
    public static <E> Iterator<E> compute(Operator<E> dataSet, int dataSetId, Partition partition, TaskContext taskContext)
    {
        int partitionId = partition.getId();
        @SuppressWarnings("unchecked")
        DataSetCache<E> dataSetCache = (DataSetCache<E>) cacheMemMap
                .computeIfAbsent(dataSetId, key -> new DataSetCache<E>(dataSetId, dataSet.numPartitions()));

        if (dataSetCache.getCache(partitionId) != null) {
            logger.debug("dataSet{}[{}] cache hit, tree: {}", dataSet, partitionId, taskContext.getDependStages());
            return dataSetCache.getCache(partitionId).prepareIterator();
        }
        logger.debug("dataSet{}[{}] cache miss, tree: {}", dataSet, partitionId, taskContext.getDependStages());

        Iterator<E> iterator = dataSet.compute(partition, taskContext);
        CacheMemory<E> partitionCacheMemory = dataSet.getRowEncoder() != null
                ? new ByteCachedMemory<>(dataSet.getRowEncoder())
                : new ObjectCacheMemory<>();
        dataSetCache.putCache(partitionId, partitionCacheMemory);
        return new Iterator<E>()
        {
            @Override
            public boolean hasNext()
            {
                boolean hasNext = iterator.hasNext();
                if (!hasNext) {
                    partitionCacheMemory.finalCache(); //后面跟随limit时存在缺陷，无法进行final
                    logger.debug("-----{} cached dep stage: {} data succeed", dataSet, taskContext.getDependStages());
                }
                return hasNext;
            }

            @Override
            public E next()
            {
                E row = iterator.next();
                partitionCacheMemory.append(row);
                return row;
            }
        };
    }
}
