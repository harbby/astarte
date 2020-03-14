package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.operator.Operator;
import com.github.harbby.ashtarte.operator.ResultStage;
import com.github.harbby.ashtarte.operator.ShuffleMapOperator;
import com.github.harbby.ashtarte.operator.ShuffleMapStage;
import com.github.harbby.ashtarte.utils.SerializableObj;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

/**
 * Local achieve
 */
public class LocalMppContext
        implements MppContext
{
    private static final Logger logger = LoggerFactory.getLogger(LocalMppContext.class);
    private static final AtomicInteger nextJobId = new AtomicInteger(1);  //发号器

    private int parallelism = 1;

    @Override
    public void setParallelism(int parallelism)
    {
        checkState(parallelism > 0, "parallelism > 0, your %s", parallelism);
        this.parallelism = parallelism;
    }

    private <E> ResultStage<E> runShuffleMapStage(Operator<E> dataSet)
    {
        List<Operator<?>> shuffleMapOperators = new ArrayList<>();

        for (Operator<?> parent = dataSet; parent != null; parent = parent.lastParent()) {
            if (parent instanceof ShuffleMapOperator) {
                shuffleMapOperators.add(parent);
            }
        }

        List<Stage> stages = new ArrayList<>();
        for (int stageId = 0; stageId < shuffleMapOperators.size(); stageId++) {
            stages.add(new ShuffleMapStage(shuffleMapOperators.get(shuffleMapOperators.size() - stageId - 1), stageId));
        }

        for (Stage stage : stages) {
            logger.info("starting... stage: {}, id {}", stage, stage.getStageId());
            SerializableObj<Stage> serializableStage = SerializableObj.of(stage);
            ExecutorService executors = Executors.newFixedThreadPool(parallelism);
            try {
                Stream.of(stage.getPartitions()).map(partition -> CompletableFuture.runAsync(() -> {
                    serializableStage.getValue().compute(partition);
                }, executors)).collect(Collectors.toList()).forEach(x -> x.join());
            }
            finally {
                executors.shutdown();
            }
        }

        return new ResultStage<>(dataSet, stages.size());   //最后一个state
    }

    @Override
    public <E, R> List<R> runJob(Operator<E> dataSet, Function<Iterator<E>, R> function)
    {
        int jobId = nextJobId.getAndIncrement();
        logger.info("starting... job: {}", jobId);
        ResultStage<E> resultStage = runShuffleMapStage(dataSet);

        SerializableObj<Operator<E>> serializableObj = SerializableObj.of(dataSet);
        ExecutorService executors = Executors.newFixedThreadPool(parallelism);
        try {
            return Stream.of(resultStage.getPartitions()).map(partition -> CompletableFuture.supplyAsync(() -> {
                Operator<E> operator = serializableObj.getValue();
                Iterator<E> iterator = operator.compute(partition, resultStage::getStageId);
                return function.apply(iterator);
            }, executors)).collect(Collectors.toList()).stream()
                    .map(x -> x.join())
                    .collect(Collectors.toList());
        }
        finally {
            executors.shutdown();
            try {
                FileUtils.deleteDirectory(new File("/tmp/shuffle"));
            }
            catch (IOException e) {
                logger.error("clear job tmp dir {} faild", "/tmp/shuffle");
            }
        }
    }
}
