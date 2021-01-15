package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.runtime.ShuffleClientManager;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public interface TaskContext
{
    int getStageId();

    Map<Integer, Integer> getDependStages();

    public ShuffleClientManager getShuffleClient();

    public String executorUUID();

    public static TaskContext of(
            int stageId,
            Map<Integer, Integer> depStages,
            ShuffleClientManager shuffleClientManager,
            String executorUUID)
    {
        Map<Integer, Integer> deps = ImmutableMap.copyOf(depStages);
        return new TaskContext()
        {
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
            public ShuffleClientManager getShuffleClient()
            {
                return shuffleClientManager;
            }

            @Override
            public String executorUUID()
            {
                return executorUUID;
            }
        };
    }
}
