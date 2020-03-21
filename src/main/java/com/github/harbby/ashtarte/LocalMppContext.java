package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.operator.Operator;
import com.github.harbby.ashtarte.operator.ShuffleMapOperator;
import com.github.harbby.ashtarte.utils.SerializableObj;
import com.github.harbby.gadtry.collection.immutable.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;
import static com.github.harbby.gadtry.base.Throwables.throwsThrowable;

/**
 * Local achieve
 */
public class LocalMppContext
        implements MppContext
{
    private static final Logger logger = LoggerFactory.getLogger(LocalMppContext.class);
    private final AtomicInteger nextJobId = new AtomicInteger(1);

    private int parallelism = 1;

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

    private <E> Map<Stage, List<Integer>> findShuffleMapOperator2(ResultStage<E> resultStage) {
        Deque<Stage> stages = new LinkedList<>();
        Deque<Operator<?>> stack = new LinkedList<>();
        stack.push(resultStage.getFinalOperator());
        //广度优先
        Map<Stage, List<Integer>> map = new LinkedHashMap<>();
        int i = resultStage.getStageId();
        Stage thisStage = resultStage;
        List<Integer> deps = new ArrayList<>();
        while (!stack.isEmpty()) {
            Operator<?> o = stack.pop();
            if (o instanceof ShuffleMapOperator) {
                map.put(thisStage, new ArrayList<>(deps));
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
        map.putIfAbsent(thisStage, new ArrayList<>(deps));
        return map;
    }

    /**
     * 广度优先
     * V3
     * */
    private Map<Stage, List<Integer>> findShuffleMapOperator(ResultStage<?> resultStage)
    {
        Map<Operator<?>, Stage> mapping = new HashMap<>();
        Map<Stage, List<Integer>> map = new LinkedHashMap<>();
        Queue<Operator<?>> stack = new LinkedList<>();

        stack.add(resultStage.getFinalOperator());
        mapping.put(resultStage.getFinalOperator(), resultStage);
        map.put(resultStage, new ArrayList<>());

        //广度优先
        int i = resultStage.getStageId();
        while (!stack.isEmpty()) {
            Operator<?> o = stack.poll();
            Stage thisStage = mapping.get(o);
            for (Operator<?> operator : o.getDependencies()) {
                if (operator instanceof ShuffleMapOperator) {
                    ShuffleMapStage newStage = new ShuffleMapStage(operator, ++i);
                    mapping.put(operator, newStage);
                    map.put(newStage, new ArrayList<>());
                    map.get(thisStage).add(i);
                }
                else {
                    mapping.put(operator, thisStage);
                }
                stack.add(operator);
            }
        }

        return map;
    }

    @Override
    public <E, R> List<R> runJob(Operator<E> dataSet, Function<Iterator<E>, R> action)
    {
        int jobId = nextJobId.getAndIncrement();
        logger.info("starting... job: {}", jobId);
        ResultStage<E> resultStage = new ResultStage<>(dataSet, 0); //  //最后一个state
        Map<Stage, List<Integer>> stageMap = findShuffleMapOperator(resultStage);

        List<Stage> stages = new ArrayList<>(stageMap.keySet());
        stages.sort((x, y) -> Integer.compare(y.getStageId(), x.getStageId()));
        new GraphScheduler(this).runGraph(stageMap);
        //---------------------
        ExecutorService executors = Executors.newFixedThreadPool(parallelism);
        try {
            FileUtils.deleteDirectory(new File("/tmp/shuffle"));
        }
        catch (IOException e) {
            throwsThrowable(e);
        }
        for (Stage stage : stages) {
            if (stage instanceof ShuffleMapStage) {
                //logger.info("starting... stage: {}, id {}", stage, stage.getStageId());
                SerializableObj<Stage> serializableStage = SerializableObj.of(stage);
                List<Integer> deps = stageMap.getOrDefault(stage, ImmutableList.of());

                Stream.of(stage.getPartitions()).map(partition -> CompletableFuture.runAsync(() -> {
                    Stage s = serializableStage.getValue();
                    s.compute(partition, TaskContext.of(s.getStageId(), deps));
                }, executors)).collect(Collectors.toList())
                        .forEach(x -> x.join());
            }
        }

        //result stage ------
        SerializableObj<ResultStage<E>> serializableObj = SerializableObj.of(resultStage);
        logger.info("starting... ResultStage: {}, id {}", resultStage, resultStage.getStageId());
        try {
            List<Integer> deps = stageMap.getOrDefault(resultStage, ImmutableList.of());
            return Stream.of(resultStage.getPartitions()).map(partition -> CompletableFuture.supplyAsync(() -> {
                Operator<E> operator = serializableObj.getValue().getFinalOperator();
                Iterator<E> iterator = operator.computeOrCache(partition,
                        TaskContext.of(resultStage.getStageId(), deps));
                return action.apply(iterator);
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
