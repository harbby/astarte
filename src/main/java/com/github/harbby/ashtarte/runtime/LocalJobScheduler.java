package com.github.harbby.ashtarte.runtime;

import com.github.harbby.ashtarte.BatchContext;
import com.github.harbby.ashtarte.JobScheduler;
import com.github.harbby.ashtarte.MapTaskState;
import com.github.harbby.ashtarte.ResultStage;
import com.github.harbby.ashtarte.ResultTask;
import com.github.harbby.ashtarte.ShuffleMapStage;
import com.github.harbby.ashtarte.ShuffleMapTask;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.Task;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.utils.SerializableObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class LocalJobScheduler
        implements JobScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(LocalJobScheduler.class);
    private final BatchContext context;

    public LocalJobScheduler(BatchContext context)
    {
        this.context = context;
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
        final ExecutorService executors = Executors.newFixedThreadPool(context.getParallelism());
        String localExecutorUUID = UUID.randomUUID().toString();
        ShuffleManagerService shuffleManagerService = new ShuffleManagerService(localExecutorUUID);
        try {
            for (Stage stage : jobStages) {
                int stageId = stage.getStageId();
                SerializableObj<Stage> serializableStage = SerializableObj.of(stage);
                Map<Integer, Integer> deps = stageMap.getOrDefault(stage, Collections.emptyMap());
                ShuffleClient shuffleClient = ShuffleClient.getLocalShuffleClient(shuffleManagerService);
                TaskContext taskContext = TaskContext.of(stageId, deps, shuffleClient, localExecutorUUID);

                if (stage instanceof ShuffleMapStage) {
                    logger.info("starting... shuffleMapStage: {}, id {}", stage, stage.getStageId());
                    Stream.of(stage.getPartitions())
                            .map(partition -> {
                                Task<MapTaskState> task = new ShuffleMapTask<>(serializableStage.getValue(), partition);
                                return task;
                            })
                            .map(task -> CompletableFuture.runAsync(() -> task.runTask(taskContext), executors))
                            .collect(Collectors.toList())
                            .forEach(CompletableFuture::join);
                }
                else {
                    //result stage ------
                    checkState(stage instanceof ResultStage, "Unknown stage " + stage);
                    logger.info("starting... ResultStage: {}, id {}", stage, stage.getStageId());
                    return Stream.of(stage.getPartitions())
                            .map(partition -> new ResultTask<>(serializableStage.getValue(), action, partition))
                            .map(task -> CompletableFuture.supplyAsync(() -> task.runTask(taskContext), executors))
                            .collect(Collectors.toList()).stream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toList());
                }
            }
        }
        finally {
            executors.shutdown();
        }
        throw new UnsupportedOperationException("job " + jobId + " Not found ResultStage");
    }
}
