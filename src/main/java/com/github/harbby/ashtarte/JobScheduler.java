package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.function.Mapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface JobScheduler
{
    public <E, R> List<R> runJob(int jobId,
            List<Stage> jobStages,
            Mapper<Iterator<E>, R> action,
            Map<Stage, Map<Integer, Integer>> stageMap) throws IOException;
}
