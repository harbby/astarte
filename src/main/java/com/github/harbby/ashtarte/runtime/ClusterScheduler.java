package com.github.harbby.ashtarte.runtime;

import com.github.harbby.ashtarte.BatchContext;
import com.github.harbby.ashtarte.ForkVmJobScheduler;
import com.github.harbby.ashtarte.JobScheduler;
import com.github.harbby.ashtarte.MapTaskState;
import com.github.harbby.ashtarte.ResultStage;
import com.github.harbby.ashtarte.ResultTask;
import com.github.harbby.ashtarte.ShuffleMapStage;
import com.github.harbby.ashtarte.ShuffleMapTask;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.Task;
import com.github.harbby.ashtarte.api.function.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class ClusterScheduler implements JobScheduler {

    private static final Logger logger = LoggerFactory.getLogger(ForkVmJobScheduler.class);
    private final BatchContext context;

    public ClusterScheduler(BatchContext context) {
        this.context = context;
    }

    @Override
    public <E, R> List<R> runJob(int jobId,
                                 List<Stage> jobStages,
                                 Mapper<Iterator<E>, R> action,
                                 Map<Stage, Map<Integer, Integer>> stageMap) throws IOException {

        for (Stage stage : jobStages) {
            submitStage(stage, action, stageMap);
        }
        throw new UnsupportedOperationException("job " + jobId + " Not found ResultStage");
    }

    public <E, R> void submitStage(Stage stage,
                                Mapper<Iterator<E>, R> action,
                                Map<Stage, Map<Integer, Integer>> stageDeps) {
        int stageId = stage.getStageId();
        Map<Integer, Integer> deps = stageDeps.getOrDefault(stage, Collections.emptyMap());
        List<Task<?>> tasks = new ArrayList<>();

        if (stage instanceof ShuffleMapStage) {
            logger.info("starting... shuffleMapStage: {}, stageId {}", stage, stage.getStageId());
            for(Partition partition : stage.getPartitions()) {
                Task<MapTaskState> task = new ShuffleMapTask<>(stage, partition);
                tasks.add(task);
            }
        } else {
            //result stage ------
            checkState(stage instanceof ResultStage, "Unknown stage " + stage);
            logger.info("starting... ResultStage: {}, stageId {}", stage, stage.getStageId());

            for(Partition partition : stage.getPartitions()) {
                Task<R> task = new ResultTask<>(stage, action, partition);
                tasks.add(task);
            }
        }

        submitTasks(tasks);
    }

    public void submitTasks(List<Task<?>> tasks) {
        System.out.println();
    }
}
