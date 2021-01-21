package com.github.harbby.astarte.runtime;

import com.github.harbby.astarte.JobScheduler;
import com.github.harbby.astarte.MapTaskState;
import com.github.harbby.astarte.ResultStage;
import com.github.harbby.astarte.ResultTask;
import com.github.harbby.astarte.ShuffleMapStage;
import com.github.harbby.astarte.ShuffleMapTask;
import com.github.harbby.astarte.TaskContext;
import com.github.harbby.astarte.api.AstarteConf;
import com.github.harbby.astarte.api.AstarteException;
import com.github.harbby.astarte.api.Stage;
import com.github.harbby.astarte.api.Task;
import com.github.harbby.astarte.api.function.Mapper;
import com.github.harbby.astarte.utils.SerializableObj;
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
                SerializableObj<Stage> serializableStage = SerializableObj.of(stage);
                Map<Integer, Integer> deps = stageMap.getOrDefault(stage, Collections.emptyMap());
                ShuffleClient shuffleClient = ShuffleClient.getLocalShuffleClient(shuffleManagerService);
                TaskContext taskContext = TaskContext.of(jobId, stageId, deps, shuffleClient, localExecutorUUID);

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
        catch (CompletionException e) {
            throw new AstarteException("local job failed", e.fillInStackTrace());
        }
        finally {
            executors.shutdown();
        }
        throw new UnsupportedOperationException("job " + jobId + " Not found ResultStage");
    }
}
