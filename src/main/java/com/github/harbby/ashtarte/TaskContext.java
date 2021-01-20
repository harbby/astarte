package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.runtime.ShuffleClient;
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
