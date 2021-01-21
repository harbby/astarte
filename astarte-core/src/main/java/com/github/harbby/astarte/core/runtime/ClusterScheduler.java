package com.github.harbby.astarte.core.runtime;

import com.github.harbby.astarte.core.JobScheduler;
import com.github.harbby.astarte.core.MapTaskState;
import com.github.harbby.astarte.core.ResultStage;
import com.github.harbby.astarte.core.ResultTask;
import com.github.harbby.astarte.core.ShuffleMapStage;
import com.github.harbby.astarte.core.ShuffleMapTask;
import com.github.harbby.astarte.core.api.AstarteConf;
import com.github.harbby.astarte.core.api.AstarteException;
import com.github.harbby.astarte.core.api.Constant;
import com.github.harbby.astarte.core.api.Partition;
import com.github.harbby.astarte.core.api.Stage;
import com.github.harbby.astarte.core.api.Task;
import com.github.harbby.astarte.core.api.function.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class ClusterScheduler
        implements JobScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterScheduler.class);
    private final ExecutorManager executorManager;
    private DriverNetManager driverNetManager;

    public ClusterScheduler(AstarteConf astarteConf, int vcores, int executorNum)
    {
        // start driver manager port
        this.driverNetManager = new DriverNetManager(astarteConf, executorNum);
        driverNetManager.start();

        int executorMemMb = astarteConf.getInt(Constant.EXECUTOR_MEMORY_CONF, 2048);

        //启动所有Executor
        this.executorManager = new ForkVmExecutorManager(vcores, executorMemMb, executorNum);
        executorManager.start();

        //wait 等待所有exector上线
        try {
            driverNetManager.awaitAllExecutorRegistered();
        }
        catch (InterruptedException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E, R> List<R> runJob(int jobId,
            List<Stage> jobStages,
            Mapper<Iterator<E>, R> action,
            Map<Stage, Map<Integer, Integer>> stageMap)
    {
        driverNetManager.initState();

        Object[] result = null;
        for (Stage stage : jobStages) {
            submitStage(stage, action, stageMap);
            if (stage instanceof ResultStage) {
                result = new Object[stage.getNumPartitions()];
            }

            //等待stage执行结束,所有task成功. 如果task失败，应重新调度一次
            //todo: 失败分为： executor挂掉, task单独失败但executor正常
            //这里采用简单的方式，先不考虑executor挂掉
            for (int taskDone = 0; taskDone < stage.getNumPartitions(); ) {
                TaskEvent taskEvent;
                try {
                    taskEvent = driverNetManager.awaitTaskEvent();
                }
                catch (InterruptedException e) {
                    throw new UnsupportedOperationException(); //todo: job kill
                }
                if (taskEvent instanceof TaskEvent.TaskFailed) {
                    TaskEvent.TaskFailed taskFailed = (TaskEvent.TaskFailed) taskEvent;
                    if (taskFailed.getJobId() != jobId) {
                        continue;
                    }
                    throw new AstarteException(((TaskEvent.TaskFailed) taskEvent).getError());
                }
                else if (taskEvent instanceof TaskEvent.TaskSuccess && stage instanceof ResultStage) {
                    TaskEvent.TaskSuccess taskSuccess = (TaskEvent.TaskSuccess) taskEvent;
                    result[taskSuccess.getTaskId()] = taskSuccess.getTaskResult();
                }
                taskDone++;
            }
            if (stage instanceof ResultStage) {
                return Arrays.asList((R[]) result);
            }
        }
        throw new UnsupportedOperationException("job " + jobId + " Not found ResultStage");
    }

    private <E, R> void submitStage(Stage stage,
            Mapper<Iterator<E>, R> action,
            Map<Stage, Map<Integer, Integer>> stageDeps)
    {
        Map<Integer, Integer> deps = stageDeps.getOrDefault(stage, Collections.emptyMap());
        stage.setDeps(deps);

        List<Task<?>> tasks = new ArrayList<>();

        if (stage instanceof ShuffleMapStage) {
            logger.info("starting... shuffleMapStage: {}, stageId {}", stage, stage.getStageId());
            for (Partition partition : stage.getPartitions()) {
                Task<MapTaskState> task = new ShuffleMapTask<>(stage, partition);
                tasks.add(task);
            }
        }
        else {
            //result stage ------
            checkState(stage instanceof ResultStage, "Unknown stage " + stage);
            logger.info("starting... ResultStage: {}, stageId {}", stage, stage.getStageId());

            for (Partition partition : stage.getPartitions()) {
                Task<R> task = new ResultTask<>(stage, action, partition);
                tasks.add(task);
            }
        }

        tasks.forEach(driverNetManager::submitTask);
    }
}
