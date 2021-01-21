package com.github.harbby.astarte;

import com.github.harbby.astarte.api.Stage;
import com.github.harbby.astarte.api.function.Mapper;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface JobScheduler
{
    public <E, R> List<R> runJob(int jobId,
            List<Stage> jobStages,
            Mapper<Iterator<E>, R> action,
            Map<Stage, Map<Integer, Integer>> stageMap);

    public default void stop()
    {
    }
}
