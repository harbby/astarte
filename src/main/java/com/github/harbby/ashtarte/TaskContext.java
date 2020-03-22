package com.github.harbby.ashtarte;

import java.util.Collections;
import java.util.List;

public interface TaskContext
{
    int getStageId();

    Integer[] getDependStages();

    public static TaskContext of(int stageId, Integer depStages)
    {
        return of(stageId, Collections.singletonList(depStages));
    }

    public static TaskContext of(int stageId, List<Integer> depStages)
    {
        Integer[] deps = depStages.toArray(new Integer[0]);
        return new TaskContext()
        {
            @Override
            public int getStageId()
            {
                return stageId;
            }

            @Override
            public Integer[] getDependStages()
            {
                return deps;
            }
        };
    }
}
