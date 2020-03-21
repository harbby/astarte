package com.github.harbby.ashtarte;

import java.util.Collections;
import java.util.List;

public interface TaskContext
{
    int getStageId();

    int[] getDependStages();

    public static TaskContext of(int stageId, int depStages)
    {
        return of(stageId, Collections.singletonList(depStages));
    }

    public static TaskContext of(int stageId, List<Integer> depStages)
    {
        int[] deps = depStages.stream().mapToInt(x -> x).toArray();
        return new TaskContext()
        {
            @Override
            public int getStageId()
            {
                return stageId;
            }

            @Override
            public int[] getDependStages()
            {
                return deps;
            }
        };
    }
}
