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

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

/**
 * CacheManager
 */
public class CacheManager
{
    private CacheManager() {}

    private static final Logger logger = LoggerFactory.getLogger(CacheManager.class);
    private static final Map<Integer, CacheMemory<?>[]> cacheMemMap = new ConcurrentHashMap<>();

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

    public static void unCacheExec(int jobId)
    {
        CacheMemory<?>[] cachedMemories = cacheMemMap.remove(jobId);
        if (cachedMemories != null) {
            for (CacheMemory byteCachedMemory : cachedMemories) {
                byteCachedMemory.freeMemory();
            }
            logger.info("cleared cache data {}", jobId);
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
            checkState(isFinal, "only reader mode");
            return data.iterator();
        }
    }

    /**
     * todo: fix bugs
     */
    public static <E> Iterator<E> compute(Operator<E> dataSet, int jobId, Partition split, TaskContext taskContext)
    {
        @SuppressWarnings("unchecked")
        CacheMemory<E>[] jobCachePartitions = (CacheMemory<E>[]) cacheMemMap.computeIfAbsent(jobId, key -> new CacheMemory[dataSet.numPartitions()]);
        if (jobCachePartitions[split.getId()] != null) {
            logger.debug("dataSet{}[{}] cache hit, tree: {}", dataSet, split.getId(), taskContext.getDependStages());
            return jobCachePartitions[split.getId()].prepareIterator();
        }
        logger.debug("dataSet{}[{}] cache miss, tree: {}", dataSet, split.getId(), taskContext.getDependStages());

        Iterator<E> iterator = dataSet.compute(split, taskContext);
        CacheMemory<E> partitionCacheMemory = dataSet.getRowEncoder() != null
                ? new ByteCachedMemory<>(dataSet.getRowEncoder())
                : new ObjectCacheMemory<>();
        return new Iterator<E>()
        {
            @Override
            public boolean hasNext()
            {
                boolean hasNext = iterator.hasNext();
                if (!hasNext) {
                    partitionCacheMemory.finalCache();
                    jobCachePartitions[split.getId()] = partitionCacheMemory;
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
