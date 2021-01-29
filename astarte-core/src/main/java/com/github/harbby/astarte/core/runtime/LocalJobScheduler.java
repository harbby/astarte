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
package com.github.harbby.astarte.core.runtime;

import com.github.harbby.astarte.core.JobScheduler;
import com.github.harbby.astarte.core.MapTaskState;
import com.github.harbby.astarte.core.ResultStage;
import com.github.harbby.astarte.core.ResultTask;
import com.github.harbby.astarte.core.ShuffleMapStage;
import com.github.harbby.astarte.core.ShuffleMapTask;
import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.api.AstarteConf;
import com.github.harbby.astarte.core.api.AstarteException;
import com.github.harbby.astarte.core.api.Stage;
import com.github.harbby.astarte.core.api.function.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class LocalJobScheduler
        implements JobScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(LocalJobScheduler.class);
    private final AstarteConf astarteConf;
    private final int parallelism;

    public LocalJobScheduler(AstarteConf astarteConf, int parallelism)
    {
        this.astarteConf = astarteConf;
        this.parallelism = parallelism;
        checkState(parallelism > 0, "local mode parallelism must > 1");
    }

    @Override
    public <E, R> List<R> runJob(
            int jobId,
            List<Stage> jobStages,
            Mapper<Iterator<E>, R> action,
            Map<Stage, Map<Integer, Integer>> stageMap)
    {
        logger.info("starting... job: {}", jobId);
        //---------------------
        final ExecutorService executors = Executors.newFixedThreadPool(parallelism);
        String localExecutorUUID = UUID.randomUUID().toString();
        ShuffleManagerService shuffleManagerService = new ShuffleManagerService(localExecutorUUID);
        shuffleManagerService.updateCurrentJobId(jobId);

        try {
            for (Stage stage : jobStages) {
                int stageId = stage.getStageId();
                Map<Integer, Integer> deps = stageMap.getOrDefault(stage, Collections.emptyMap());
                ShuffleClient shuffleClient = ShuffleClient.getLocalShuffleClient(shuffleManagerService);
                TaskContext taskContext = TaskContext.of(jobId, stageId, deps, shuffleClient, localExecutorUUID);

                if (stage instanceof ShuffleMapStage) {
                    logger.info("starting... shuffleMapStage: {}, id {}", stage, stage.getStageId());
                    Stream.of(stage.getPartitions())
                            .map(partition -> new ShuffleMapTask<MapTaskState>(stage, partition))
                            .map(task -> CompletableFuture.runAsync(() -> task.runTask(taskContext), executors))
                            .collect(Collectors.toList())
                            .forEach(CompletableFuture::join);
                }
                else {
                    //result stage ------
                    checkState(stage instanceof ResultStage, "Unknown stage " + stage);
                    logger.info("starting... ResultStage: {}, id {}", stage, stage.getStageId());
                    return Stream.of(stage.getPartitions())
                            .map(partition -> new ResultTask<>(stage, action, partition))
                            .map(task -> CompletableFuture.supplyAsync(() -> task.runTask(taskContext), executors))
                            .collect(Collectors.toList()).stream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toList());
                }
            }
        }
        catch (CompletionException e) {
            throw new AstarteException("local job failed", e.fillInStackTrace());
        }
        finally {
            executors.shutdown();
        }
        throw new UnsupportedOperationException("job " + jobId + " Not found ResultStage");
    }
}
