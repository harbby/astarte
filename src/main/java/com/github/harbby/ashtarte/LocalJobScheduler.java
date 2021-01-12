package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.Task;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.operator.Operator;
import com.github.harbby.ashtarte.utils.SerializableObj;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class LocalJobScheduler
        implements JobScheduler {
    private static final Logger logger = LoggerFactory.getLogger(LocalJobScheduler.class);
    private final BatchContext context;

    public LocalJobScheduler(BatchContext context) {
        this.context = context;
    }

    @Override
    public <E, R> List<R> runJob(
            int jobId,
            List<Stage> jobStages,
            Mapper<Iterator<E>, R> action,
            Map<Stage, Map<Integer, Integer>> stageMap)
            throws IOException {
        logger.info("starting... job: {}", jobId);
        //---------------------
        FileUtils.deleteDirectory(new File("/tmp/shuffle"));
        final ExecutorService executors = Executors.newFixedThreadPool(context.getParallelism());
        try {
            for (Stage stage : jobStages) {
                int stageId = stage.getStageId();
                SerializableObj<Stage> serializableStage = SerializableObj.of(stage);
                Map<Integer, Integer> deps = stageMap.getOrDefault(stage, Collections.emptyMap());
                TaskContext taskContext = TaskContext.of(stageId, deps);

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
                } else {
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
        } finally {
            try {
                FileUtils.deleteDirectory(new File("/tmp/shuffle"));
            } catch (IOException e) {
                logger.error("clear job tmp dir {} faild", "/tmp/shuffle");
            } finally {
                executors.shutdown();
            }
        }
        throw new UnsupportedOperationException("job " + jobId + " Not found ResultStage");
    }
}
