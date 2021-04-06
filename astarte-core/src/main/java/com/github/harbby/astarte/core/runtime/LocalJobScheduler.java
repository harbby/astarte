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
import com.github.harbby.astarte.core.ResultStage;
import com.github.harbby.astarte.core.ResultTask;
import com.github.harbby.astarte.core.ShuffleMapStage;
import com.github.harbby.astarte.core.ShuffleMapTask;
import com.github.harbby.astarte.core.TaskContext;
import com.github.harbby.astarte.core.Utils;
import com.github.harbby.astarte.core.api.AstarteException;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.Stage;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.operator.Operator;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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
        extends JobScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(LocalJobScheduler.class);
    private final int parallelism;

    public LocalJobScheduler(int parallelism)
    {
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
        File shuffleWorkDir = new File(System.getProperty("java.io.tmpdir"), "astarte-" + UUID.randomUUID().toString());
        try {
            for (Stage stage : jobStages) {
                int stageId = stage.getStageId();
                ShuffleClient shuffleClient = new ShuffleClient.LocalShuffleClient(shuffleWorkDir, jobId);
                TaskContext taskContext = TaskContext.of(jobId, stageId, stageMap.get(stage), shuffleClient, shuffleWorkDir);
                logger.info("starting stage {}/{} {}", stage.getStageId(), jobStages.size(), stage);
                Partition[] partitions = Utils.clear(stage.getPartitions());
                if (stage instanceof ShuffleMapStage) {
                    Stream.of(partitions)
                            .map(partition -> new ShuffleMapTask(jobId, stageId, partition, ((ShuffleMapStage) stage).getFinalOperator(), Collections.emptyMap(), stageMap.get(stage)))
                            .map(task -> CompletableFuture.runAsync(() -> task.runTask(taskContext), executors))
                            .collect(Collectors.toList())
                            .forEach(CompletableFuture::join);
                }
                else {
                    //result stage ------
                    checkState(stage instanceof ResultStage, "Unknown stage " + stage);
                    return Stream.of(partitions)
                            .map(partition -> new ResultTask<>(jobId, stageId, (Operator<E>) stage.getFinalOperator(), action, partition, Collections.emptyMap(), stageMap.get(stage)))
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
            try {
                FileUtils.deleteDirectory(shuffleWorkDir);
                logger.debug("clear shuffle data temp dir {}", shuffleWorkDir);
            }
            catch (IOException e) {
                logger.error("clear shuffle data temp dir failed", e);
            }
        }
        throw new UnsupportedOperationException("job " + jobId + " Not found ResultStage");
    }

    @Override
    public void stop()
    {
    }
}
