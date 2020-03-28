package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Stage;
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
            throws IOException
    {
        logger.info("starting... job: {}", jobId);
        //---------------------
        FileUtils.deleteDirectory(new File("/tmp/shuffle"));
        final ExecutorService executors = Executors.newFixedThreadPool(context.getParallelism());
        try {
            for (Stage stage : jobStages) {
                SerializableObj<Stage> serializableStage = SerializableObj.of(stage);
                if (stage instanceof ShuffleMapStage) {
                    logger.info("starting... shuffleMapStage: {}, id {}", stage, stage.getStageId());
                    Map<Integer, Integer> deps = stageMap.getOrDefault(stage, Collections.emptyMap());

                    Stream.of(stage.getPartitions()).map(partition -> CompletableFuture.runAsync(() -> {
                        Stage s = serializableStage.getValue();
                        s.compute(partition, TaskContext.of(s.getStageId(), deps));
                    }, executors)).collect(Collectors.toList())
                            .forEach(CompletableFuture::join);
                }
                else {
                    //result stage ------
                    checkState(stage instanceof ResultStage, "Unknown stage " + stage);
                    logger.info("starting... ResultStage: {}, id {}", stage, stage.getStageId());
                    Map<Integer, Integer> deps = stageMap.getOrDefault(stage, Collections.emptyMap());
                    return Stream.of(stage.getPartitions()).map(partition -> CompletableFuture.supplyAsync(() -> {
                        @SuppressWarnings("unchecked")
                        ResultStage<E> resultStage = (ResultStage<E>) serializableStage.getValue();
                        Operator<E> operator = resultStage.getFinalOperator();
                        Iterator<E> iterator = operator.computeOrCache(partition,
                                TaskContext.of(resultStage.getStageId(), deps));
                        return action.map(iterator);
                    }, executors)).collect(Collectors.toList()).stream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toList());
                }
            }
        }
        finally {
            try {
                FileUtils.deleteDirectory(new File("/tmp/shuffle000"));
            }
            catch (IOException e) {
                logger.error("clear job tmp dir {} faild", "/tmp/shuffle");
            }
            finally {
                executors.shutdown();
            }
        }
        throw new UnsupportedOperationException("job " + jobId + " Not found ResultStage");
    }
}
