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

import com.github.harbby.astarte.core.runtime.ShuffleClient;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public interface TaskContext
{
    int getJobId();

    int getStageId();

    Map<Integer, Integer> getDependStages();

    public ShuffleClient getShuffleClient();

    public String executorUUID();

    public static TaskContext of(
            int jobId,
            int stageId,
            Map<Integer, Integer> depStages,
            ShuffleClient shuffleClient,
            String executorUUID)
    {
        Map<Integer, Integer> deps = ImmutableMap.copyOf(depStages);
        return new TaskContext()
        {
            @Override
            public int getJobId()
            {
                return jobId;
            }

            @Override
            public int getStageId()
            {
                return stageId;
            }

            @Override
            public Map<Integer, Integer> getDependStages()
            {
                return deps;
            }

            @Override
            public ShuffleClient getShuffleClient()
            {
                return shuffleClient;
            }

            @Override
            public String executorUUID()
            {
                return executorUUID;
            }
        };
    }
}
