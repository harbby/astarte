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
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
    private final AtomicInteger nextJobId = new AtomicInteger(0);  //发号器
    private final AtomicInteger nextShuffleId = new AtomicInteger(0);

    private int parallelism = 1;

    public int newShuffleId()
    {
        return nextShuffleId.getAndIncrement();
    }

    @Override
    public void setParallelism(int parallelism)
    {
        checkState(parallelism > 0, "parallelism > 0, your %s", parallelism);
        this.parallelism = parallelism;
    }

    private <E> List<Operator<?>> findShuffleMapOperator1(Operator<E> dataSet)
    {
        List<Operator<?>> shuffleMapOperators = new ArrayList<>();
        Deque<Operator<?>> stack = new LinkedList<>();
        stack.push(dataSet);
        //广度优先
        while (!stack.isEmpty()) {
            Operator<?> o = stack.pop();
            for (Operator<?> operator : o.getDependencies()) {
                if (operator instanceof ShuffleMapOperator) {
                    shuffleMapOperators.add(operator);
                }
                stack.push(operator);
            }
        }
        return shuffleMapOperators;
    }

    /**
     * 使用栈结构 可以优化递归算法
     */

    private <E> Map<Stage, Integer[]> findShuffleMapOperator(ResultStage<E> resultStage)
    {
        Deque<Stage> stages = new LinkedList<>();
        Deque<Operator<?>> stack = new LinkedList<>();
        stack.push(resultStage.getFinalOperator());
        //广度优先
        Map<Stage, Integer[]> map = new LinkedHashMap<>();
        int i = resultStage.getStageId();
        Stage thisStage = resultStage;
        List<Integer> deps = new ArrayList<>();
        while (!stack.isEmpty()) {
            Operator<?> o = stack.pop();
            if (o instanceof ShuffleMapOperator) {
                map.put(thisStage, deps.toArray(new Integer[0]));
                deps.clear();
                thisStage = stages.pop();
            }
            for (Operator<?> operator : o.getDependencies()) {
                if (operator instanceof ShuffleMapOperator) {
                    stages.push(new ShuffleMapStage(operator, ++i));
                    deps.add(i);
                }
                stack.push(operator);
            }
        }
        map.putIfAbsent(thisStage, deps.toArray(new Integer[0]));
        return map;
    }

    @Override
    public <E, R> List<R> runJob(Operator<E> dataSet, Function<Iterator<E>, R> function)
    {
        int jobId = nextJobId.getAndIncrement();
        logger.info("starting... job: {}", jobId);
        ResultStage<E> resultStage = new ResultStage<>(dataSet, 0); //  //最后一个state
        Map<Stage, Integer[]> stageMap = findShuffleMapOperator(resultStage);

        List<Stage> stages = new ArrayList<>(stageMap.keySet());
        Collections.reverse(stages);

        //---------------------
        ExecutorService executors = Executors.newFixedThreadPool(parallelism);
        for (Stage stage : stages) {
            if (stage instanceof ShuffleMapStage) {
                logger.info("starting... stage: {}, id {}", stage, stage.getStageId());
                SerializableObj<Stage> serializableStage = SerializableObj.of(stage);
                Integer[] deps = stageMap.getOrDefault(stage, new Integer[0]);

                Stream.of(stage.getPartitions()).map(partition -> CompletableFuture.runAsync(() -> {
                    Stage s = serializableStage.getValue();
                    s.compute(partition, TaskContext.of(s.getStageId(), deps));
                }, executors)).collect(Collectors.toList())
                        .forEach(x -> x.join());
            }
        }

        //result stage ------
        SerializableObj<ResultStage<E>> serializableObj = SerializableObj.of(resultStage);
        try {
            Integer[] deps = stageMap.getOrDefault(resultStage, new Integer[0]);
            return Stream.of(resultStage.getPartitions()).map(partition -> CompletableFuture.supplyAsync(() -> {
                Operator<E> operator = serializableObj.getValue().getFinalOperator();
                Iterator<E> iterator = operator.compute(partition,
                        TaskContext.of(resultStage.getStageId(), deps));
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
