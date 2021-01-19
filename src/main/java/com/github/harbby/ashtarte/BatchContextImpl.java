package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.AshtarteConf;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.operator.Operator;
import com.github.harbby.ashtarte.operator.ShuffleMapOperator;
import com.github.harbby.ashtarte.runtime.ClusterScheduler;
import com.github.harbby.gadtry.graph.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.harbby.gadtry.base.MoreObjects.checkArgument;
import static com.github.harbby.gadtry.base.MoreObjects.checkState;

/**
 * Local achieve
 */
public class BatchContextImpl
        implements BatchContext
{
    private static final Logger logger = LoggerFactory.getLogger(BatchContextImpl.class);
    private final AtomicInteger nextJobId = new AtomicInteger(1);
    private final AshtarteConf conf = new AshtarteConf();

    private final JobScheduler jobScheduler = new ClusterScheduler(this);  //LocalJobScheduler

    private int parallelism = 1;

    @Override
    public AshtarteConf getConf()
    {
        return conf;
    }

    @Override
    public void setParallelism(int parallelism)
    {
        checkState(parallelism > 0, "parallelism > 0, your %s", parallelism);
        this.parallelism = parallelism;
    }

    @Override
    public int getParallelism()
    {
        return parallelism;
    }

    @Override
    public <E, R> List<R> runJob(Operator<E> finalOperator, Mapper<Iterator<E>, R> action)
    {
        checkArgument(!(finalOperator instanceof KvDataSet), "use unboxing(this)");
        int jobId = nextJobId.getAndIncrement();
        logger.info("begin analysis job {} deps to stageDAG", jobId);

        Map<Stage, Map<Integer, Integer>> stageMap = findShuffleMapOperator(finalOperator);

        List<Stage> stages = new ArrayList<>(stageMap.keySet());
        stages.sort((x, y) -> Integer.compare(y.getStageId(), x.getStageId()));

        Graph<Stage, Void> graph = toGraph(stageMap);
        if (stages.size() < 10) {
            logger.info("job graph tree:{}", String.join("\n", graph.printShow()));
        }
        //---------------------
        return jobScheduler.runJob(jobId, stages, action, stageMap);
    }

    /**
     * V5
     */
    private Map<Stage, Map<Integer, Integer>> findShuffleMapOperator(Operator<?> finalDataSet)
    {
        Map<Operator<?>, Stage> mapping = new HashMap<>();
        //Map<thisStage, Map<shuffleMapId, shuffleMapStage>>
        Map<Stage, Map<Integer, Integer>> map = new LinkedHashMap<>();
        Queue<Operator<?>> stack = new LinkedList<>();

        Stage resultStage = new ResultStage<>(finalDataSet, 0);
        stack.add(finalDataSet);
        mapping.put(resultStage.getFinalOperator(), resultStage);
        map.put(resultStage, new LinkedHashMap<>());

        Map<Operator<?>, Set<Stage>> markCached = new LinkedHashMap<>();
        //广度优先
        int i = resultStage.getStageId();
        while (!stack.isEmpty()) {
            Operator<?> o = stack.poll();
            Stage thisStage = mapping.get(o);

            List<? extends Operator<?>> depOperators;
            if (o.isMarkedCache()) {
                //put(op, thisStage) , save thisStage dep markedOperator
                markCached.computeIfAbsent(o, k -> new HashSet<>()).add(thisStage);
                depOperators = Collections.emptyList();
            }
            else {
                depOperators = o.getDependencies();
            }
            for (Operator<?> operator : depOperators) {
                if (operator instanceof ShuffleMapOperator) {
                    Map<Integer, Integer> stageDeps = map.get(thisStage);
                    Integer operatorDependStage = stageDeps.get(operator.getId());
                    if (operatorDependStage != null && operatorDependStage != i + 1) {
                        logger.info("find 当前相同的shuffleMapStage,将优化为只有一个");
                        continue;
                    }

                    ShuffleMapStage newStage = new ShuffleMapStage((ShuffleMapOperator<?, ?>) operator, ++i);
                    mapping.put(operator, newStage);
                    map.put(newStage, new LinkedHashMap<>());
                    stageDeps.put(operator.getId(), i);
                }
                else {
                    mapping.put(operator, thisStage);
                }
                stack.add(operator);
            }

            // cached Operator Analysis ---
            if (stack.isEmpty() && !markCached.isEmpty()) {
                Iterator<Map.Entry<Operator<?>, Set<Stage>>> markCachedIterator = markCached.entrySet().iterator();
                Map.Entry<Operator<?>, Set<Stage>> entry = markCachedIterator.next();
                Operator<?> markCachedOperator = entry.getKey();
                markCachedIterator.remove();
                for (Operator<?> child : markCachedOperator.getDependencies()) {
                    stack.add(child);
                    checkState(!(child instanceof ShuffleMapOperator), "推测失败");
                    mapping.put(child, thisStage);  //这里凭感觉推测,不可能是 ShuffleMapOperator

                    //---------递归推测cached Operator的前置依赖
                    //下面的推断不是必须的，但是推断后可以让dag show的时候更加清晰,好看.
                    Map<Stage, Map<Integer, Integer>> markedDeps = findShuffleMapOperator(child);
                    if (!markedDeps.isEmpty()) {
                        for (Stage stage : entry.getValue()) {
                            Map<Integer, Integer> mergedDeps = new LinkedHashMap<>();
                            //这里next 我们只取地一个
                            for (Map.Entry<Integer, Integer> entry1 : markedDeps.values().iterator().next().entrySet()) {
                                mergedDeps.put(entry1.getKey(), i + entry1.getValue());
                            }
                            mergedDeps.putAll(map.get(stage));
                            map.put(stage, mergedDeps);
                        }
                    }
                }
                logger.info("begin analysis markCachedOperator {}", markCachedOperator);
            }
        }

        return map;
    }

    public static Graph<Stage, Void> toGraph(Map<Stage, ? extends Map<Integer, Integer>> stages)
    {
        Graph.GraphBuilder<Stage, Void> builder = Graph.builder();
        for (Stage stage : stages.keySet()) {
            builder.addNode(stage.getStageId() + "", stage);
        }

        for (Map.Entry<Stage, ? extends Map<Integer, Integer>> entry : stages.entrySet()) {
            for (int id : entry.getValue().values()) {
                builder.addEdge(entry.getKey().getStageId() + "", id + "");
            }
        }
        Graph<Stage, Void> graph = builder.create();
        //graph.printShow().forEach(x -> System.out.println(x));
        return graph;
    }
}
