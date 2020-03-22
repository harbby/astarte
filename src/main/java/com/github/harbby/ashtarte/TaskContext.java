package com.github.harbby.ashtarte;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public interface TaskContext
{
    int getStageId();

    Map<Integer, Integer> getDependStages();

    public static TaskContext of(int stageId, Map<Integer, Integer> depStages)
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
        };
    }
}
