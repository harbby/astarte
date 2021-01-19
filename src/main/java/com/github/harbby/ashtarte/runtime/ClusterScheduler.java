package com.github.harbby.ashtarte.runtime;

import com.github.harbby.ashtarte.BatchContext;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class ClusterScheduler
        implements JobScheduler
{

    private static final Logger logger = LoggerFactory.getLogger(ClusterScheduler.class);
    private final BatchContext context;
    private final ExecutorManager executorManager;
    private DriverNetManager driverNetManager;

    public ClusterScheduler(BatchContext context)
    {
        this.context = context;
        // start driver manager port
        int executorNum = 2;

        this.driverNetManager = new DriverNetManager(context.getConf(), executorNum);
        driverNetManager.start();

        //启动所有Executor
        this.executorManager = new ForkVmExecutorManager(2, 2048, executorNum);
        executorManager.start();

        //wait 等待所有exector上线
        try {
            driverNetManager.awaitAllExecutorRegistered();
        }
        catch (InterruptedException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    @Override
    public <E, R> List<R> runJob(int jobId,
            List<Stage> jobStages,
            Mapper<Iterator<E>, R> action,
            Map<Stage, Map<Integer, Integer>> stageMap)
    {
        List<R> rs = new ArrayList<>();
        for (Stage stage : jobStages) {
            submitStage(stage, action, stageMap);
            //等待stage执行结束,所有task成功. 如果task失败，应重新调度一次
            //todo: 失败分为： executor挂掉, task单独失败但executor正常
            for (int i = 0; i < stage.getNumPartitions(); i++) {
                TaskEvent taskEvent = null;
                try {
                    taskEvent = driverNetManager.awaitTaskEvent();
                }
                catch (InterruptedException e) {
                    //todo: job kill
                    throw new UnsupportedOperationException();
                }
                if (stage instanceof ResultStage) {
                    rs.add((R) taskEvent.getTaskResult());
                }
            }
            if (stage instanceof ResultStage) {
                return rs;
            }
        }
        throw new UnsupportedOperationException("job " + jobId + " Not found ResultStage");
    }

    private  <E, R> void submitStage(Stage stage,
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
