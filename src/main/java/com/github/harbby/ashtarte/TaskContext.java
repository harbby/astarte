package com.github.harbby.ashtarte;

import java.util.stream.Stream;

public interface TaskContext
{
    int getStageId();

    int[] getDependStages();

    public static TaskContext of(int stageId, Integer[] depStages)
    {
        int[] intArray = Stream.of(depStages).mapToInt(x -> x).toArray();
        return of(stageId, intArray);
    }

    public static TaskContext of(int stageId, int[] depStages)
    {
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
                return depStages;
            }
        };
    }
}
