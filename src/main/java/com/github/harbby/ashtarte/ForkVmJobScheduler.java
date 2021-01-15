package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.Task;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.runtime.DriverNetManager;
import com.github.harbby.ashtarte.runtime.TaskEvent;
import com.github.harbby.ashtarte.runtime.TaskManager;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import com.github.harbby.gadtry.jvm.VmFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class ForkVmJobScheduler
        implements JobScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(ForkVmJobScheduler.class);
    private final BatchContext context;
    private DriverNetManager driverNetManager;
    private List<VmFuture<Integer>> vms;

    public ForkVmJobScheduler(BatchContext context)
    {
        this.context = context;
        // start driver manager port
        DriverNetManager driverNetManager = new DriverNetManager();
        driverNetManager.start();
        this.driverNetManager = driverNetManager;

        //启动所有Executor
        this.vms = IntStream.range(0, 1).mapToObj(x -> {
            return JVMLaunchers.<Integer>newJvm()
                    .setName("ashtarte.Executor")
                    .task(() -> {
                        System.out.println("starting... Executor");
                        TaskManager.main(new String[0]);
                        return 0;
                    })
                    .setConsole(System.out::print)
                    .addVmOps("-Dio.netty.leakDetectionLevel=advanced")
                    .setXmx("2048m")
                    .build()
                    .startAsync(Executors.newSingleThreadExecutor());
        }).collect(Collectors.toList());
        //wait 等待所有exector上线
        while (driverNetManager.handlerMap.size() != vms.size()) {
            try {
                TimeUnit.MILLISECONDS.sleep(10);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("all executor({}) init Initialized", vms.size());
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

            stage.setShuffleServices(new HashSet<>(driverNetManager.handlerMap.keySet()));
            if (stage instanceof ShuffleMapStage) {
                logger.info("starting... shuffleMapStage: {}, id {}", stage, stage.getStageId());
                for (Partition partition : stage.getPartitions()) {
                    Task<MapTaskState> task = new ShuffleMapTask<>(stage, partition);
                    driverNetManager.handlerMap.values().stream().findAny().get()  //调度策略暂时为　随机调度
                            .submitTask(task);
                }
            }
            else {
                //result stage ------
                checkState(stage instanceof ResultStage, "Unknown stage " + stage);
                logger.info("starting... ResultStage: {}, id {}", stage, stage.getStageId());
                for (Partition partition : stage.getPartitions()) {
                    ResultTask<E, R> task = new ResultTask<>(stage, action, partition);
                    driverNetManager.handlerMap.values().stream().findAny().get()
                            .submitTask(task);
                }
            }
            //todo: 等待stage执行结束 await()
            for (int i = 0; i < stage.getNumPartitions(); i++) {
                TaskEvent taskEvent = null;
                try {
                    taskEvent = driverNetManager.queue.take();
                }
                catch (InterruptedException e) {
                    //todo: job kill
                    e.printStackTrace();
                }
                if (stage instanceof ResultStage) {
                    rs.add((R) taskEvent.getTaskResult());
                }
            }
            //---------------------
            //todo: 如果失败则重新调度该stage
        });
        //this.stop();
        return rs;
    }

    @Override
    public void stop()
    {
        driverNetManager.handlerMap.clear();
        vms.forEach(x -> x.cancel());
    }
}
