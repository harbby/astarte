package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.Task;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.runtime.DriverNetManager;
import com.github.harbby.ashtarte.runtime.ExecutorManager;
import com.github.harbby.ashtarte.runtime.ForkVmExecutorManager;
import com.github.harbby.ashtarte.runtime.TaskEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class ForkVmJobScheduler
        implements JobScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(ForkVmJobScheduler.class);
    private final BatchContext context;
    private final ExecutorManager executorManager;
    private DriverNetManager driverNetManager;

    public ForkVmJobScheduler(BatchContext context)
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
            throws IOException
    {
        logger.info("starting... job: {}", jobId);
        List<R> rs = new ArrayList<>();
        jobStages.forEach(stage -> {
            Map<Integer, Integer> deps = stageMap.getOrDefault(stage, Collections.emptyMap());
            stage.setDeps(deps);
            //todo: found first stage, 读数据本地化
            if (stage instanceof ShuffleMapStage) {
                logger.info("starting... shuffleMapStage: {}, id {}", stage, stage.getStageId());
                for (Partition partition : stage.getPartitions()) {
                    Task<MapTaskState> task = new ShuffleMapTask<>(stage, partition);
                    driverNetManager.submitTask(task);
                }
            }
            else {
                checkState(stage instanceof ResultStage, "Unknown stage " + stage);
                logger.info("starting... ResultStage: {}, id {}", stage, stage.getStageId());
                for (Partition partition : stage.getPartitions()) {
                    ResultTask<E, R> task = new ResultTask<>(stage, action, partition);
                    driverNetManager.submitTask(task);
                }
            }

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
        });
        return rs;
    }

    @Override
    public void stop()
    {
        driverNetManager.stop();
        executorManager.stop();
    }
}
