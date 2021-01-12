package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.Task;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.utils.SerializableObj;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class ForkVmJobScheduler
        implements JobScheduler {
    private static final Logger logger = LoggerFactory.getLogger(ForkVmJobScheduler.class);
    private final BatchContext context;

    public ForkVmJobScheduler(BatchContext context) {
        this.context = context;
    }

    @Override
    public <E, R> List<R> runJob(int jobId,
                                 List<Stage> jobStages,
                                 Mapper<Iterator<E>, R> action,
                                 Map<Stage, Map<Integer, Integer>> stageMap)
            throws IOException {
        logger.info("starting... job: {}", jobId);
        //---------------------
        FileUtils.deleteDirectory(new File("/tmp/shuffle"));
        try {
            for (Stage stage : jobStages) {
                int stageId = stage.getStageId();
                Map<Integer, Integer> deps = stageMap.getOrDefault(stage, Collections.emptyMap());

                if (stage instanceof ShuffleMapStage) {
                    logger.info("starting... shuffleMapStage: {}, stageId {}", stage, stage.getStageId());
                    Stream.of(stage.getPartitions()).forEach(partition -> {
                        Task<MapTaskState> task = new ShuffleMapTask<>(stage, partition);

                        JVMLauncher<String> jvmLauncher = JVMLaunchers.<String>newJvm()
                                .setCallable(() -> {
                                    TaskContext taskContext = TaskContext.of(stageId, deps);
                                    task.runTask(taskContext);  //return metaData
                                    return null;
                                })
                                .setConsole(System.out::println)
                                .build();
                        jvmLauncher.startAndGet();
                    });
                } else {
                    //result stage ------
                    checkState(stage instanceof ResultStage, "Unknown stage " + stage);
                    logger.info("starting... ResultStage: {}, stageId {}", stage, stage.getStageId());
                    return Stream.of(stage.getPartitions()).map(partition -> {
                        Task<R> task = new ResultTask<>(stage, action, partition);

                        JVMLauncher<Serializable> jvmLauncher = JVMLaunchers.newJvm()
                                .setCallable(() -> {
                                    TaskContext taskContext = TaskContext.of(stageId, deps);
                                    return (Serializable) task.runTask(taskContext);
                                })
                                .setConsole(System.out::println)
                                .build();
                        return (R) jvmLauncher.startAndGet();
                    }).collect(Collectors.toList());
                }
            }
        } finally {
            try {
                FileUtils.deleteDirectory(new File("/tmp/shuffle000"));
            } catch (IOException e) {
                logger.error("clear job tmp dir {} faild", "/tmp/shuffle");
            }
        }
        throw new UnsupportedOperationException("job " + jobId + " Not found ResultStage");
    }
}
