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
package com.github.harbby.astarte.core;

import com.github.harbby.astarte.core.api.AstarteConf;
import com.github.harbby.astarte.core.api.KvDataSet;
import com.github.harbby.astarte.core.api.Stage;
import com.github.harbby.astarte.core.api.function.Mapper;
import com.github.harbby.astarte.core.operator.Operator;
import com.github.harbby.astarte.core.operator.ShuffleMapOperator;
import com.github.harbby.gadtry.collection.ImmutableList;
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
import java.util.stream.Collectors;

import static com.github.harbby.gadtry.base.MoreObjects.checkArgument;

/**
 * Local achieve
 */
class BatchContextImpl
        implements BatchContext
{
    private static final Logger logger = LoggerFactory.getLogger(BatchContextImpl.class);
    private final AtomicInteger nextJobId = new AtomicInteger(1);
    private final AstarteConf conf;

    private final JobScheduler jobScheduler;  //LocalJobScheduler

    public BatchContextImpl(AstarteConf conf)
    {
        this.conf = conf;
        this.jobScheduler = JobScheduler.createJobScheduler(conf);
    }

    @Override
    public void stop()
    {
        jobScheduler.stop();
    }

    @Override
    public AstarteConf getConf()
    {
        return conf;
    }

    @Override
    public <E, R> List<R> runJob(Operator<E> finalOperator, Mapper<Iterator<E>, R> action)
    {
        checkArgument(!(finalOperator instanceof KvDataSet), "use unboxing(this)");
        int jobId = nextJobId.getAndIncrement();
        logger.info("begin analysis job {} deps to stageDAG", jobId);

        Map<Operator<?>, Set<Integer>> stageTree = parserTree(finalOperator);
        Map<Stage, Map<Integer, Integer>> optimizedDag = optimizer(jobId, stageTree);

        Graph<Integer, Void> graph = toGraph(optimizedDag);
        if (optimizedDag.size() < 10) {
            logger.info("job graph tree:{}", String.join("\n", graph.printShow()));
        }
        //---------------------
        return jobScheduler.runJob(jobId, ImmutableList.copy(optimizedDag.keySet()), action, optimizedDag);
    }

    private static Map<Operator<?>, Set<Integer>> parserTree(Operator<?> finalDataSet)
    {
        Map<Operator<?>, Operator<?>> mapping = new HashMap<>();
        Map<Operator<?>, Set<Integer>> stageTree = new LinkedHashMap<>();
        Queue<Operator<?>> stack = new LinkedList<>();

        stack.add(finalDataSet);
        mapping.put(finalDataSet, finalDataSet);
        stageTree.put(finalDataSet, new HashSet<>());

        while (!stack.isEmpty()) {
            Operator<?> o = stack.poll();
            Operator<?> currentStage = mapping.getOrDefault(o, o);

            List<? extends Operator<?>> depOperators = o.getDependencies();
            for (Operator<?> operator : depOperators) {
                if (operator instanceof ShuffleMapOperator) {
                    Set<Integer> stageDeps = stageTree.get(currentStage);
                    stageDeps.add(operator.getId());
                    Set<Integer> set = stageTree.remove(operator);
                    stageTree.put(operator, set == null ? new HashSet<>() : set);
                }
                else {
                    mapping.put(operator, currentStage);
                }
                stack.add(operator);
            }
        }
        return stageTree;
    }

    private static Map<Stage, Map<Integer, Integer>> optimizer(int jobId, Map<Operator<?>, Set<Integer>> stageTree)
    {
        List<Map.Entry<Operator<?>, Set<Integer>>> stages = new ArrayList<>(stageTree.entrySet());
        Collections.reverse(stages);
        Map<Stage, Map<Integer, Integer>> stageMap = new LinkedHashMap<>();
        Map<Integer, Integer> mapping = new HashMap<>();
        int stageId = 1;
        for (Map.Entry<Operator<?>, Set<Integer>> entry : stages) {
            Stage stage;
            if (entry.getKey() instanceof ShuffleMapOperator) {
                ((ShuffleMapOperator<?, ?>) entry.getKey()).setStageId(stageId);
                stage = new ShuffleMapStage((ShuffleMapOperator<?, ?>) entry.getKey(), jobId, stageId++);
                mapping.put(entry.getKey().getId(), stage.getStageId());
            }
            else {
                stage = new ResultStage<>(entry.getKey(), jobId, stageId++);
            }
            stageMap.put(stage, entry.getValue().stream().collect(Collectors.toMap(k -> k, mapping::get)));
        }
        return stageMap;
    }

    public static Graph<Integer, Void> toGraph(Map<Stage, ? extends Map<Integer, Integer>> stages)
    {
        Graph.GraphBuilder<Integer, Void> builder = Graph.builder();
        for (Stage stage : stages.keySet()) {
            builder.addNode(stage.getStageId());
        }

        for (Map.Entry<Stage, ? extends Map<Integer, Integer>> entry : stages.entrySet()) {
            for (int id : entry.getValue().values()) {
                builder.addEdge(entry.getKey().getStageId(), id);
            }
        }
        //graph.printShow().forEach(x -> System.out.println(x));
        return builder.create();
    }
}
