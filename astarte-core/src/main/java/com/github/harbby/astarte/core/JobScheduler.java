package com.github.harbby.astarte.core;

import com.github.harbby.astarte.core.api.Stage;
import com.github.harbby.astarte.core.api.function.Mapper;

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