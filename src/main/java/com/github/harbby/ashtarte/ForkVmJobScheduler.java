package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.ashtarte.api.function.Mapper;
import com.github.harbby.ashtarte.operator.Operator;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class ForkVmJobScheduler
        implements JobScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(ForkVmJobScheduler.class);
    private final BatchContext context;

    public ForkVmJobScheduler(BatchContext context)
    {
        this.context = context;
    }

    @Override
    public <E, R> List<R> runJob(int jobId,
            List<Stage> jobStages,
            Mapper<Iterator<E>, R> action,
            Map<Stage, Map<Integer, Integer>> stageMap)
            throws IOException
    {
        logger.info("starting... job: {}", jobId);
        //---------------------
        FileUtils.deleteDirectory(new File("/tmp/shuffle"));
        final ExecutorService executors = Executors.newFixedThreadPool(context.getParallelism());
        try {
            for (Stage stage : jobStages) {
                SerializableObj<Stage> serializableStage = SerializableObj.of(stage);
                if (stage instanceof ShuffleMapStage) {
                    logger.info("starting... shuffleMapStage: {}, stageId {}", stage, stage.getStageId());
                    Map<Integer, Integer> deps = stageMap.getOrDefault(stage, Collections.emptyMap());
                    Stream.of(stage.getPartitions()).forEach(partition -> {
                        JVMLauncher<String> jvmLauncher = JVMLaunchers.<String>newJvm()
                                .setCallable(() -> {
                                    Stage s = serializableStage.getValue();
                                    s.compute(partition, TaskContext.of(s.getStageId(), deps));
                                    return null; //return metaData
                                })
                                .setConsole(System.out::println)
                                .build();
                        jvmLauncher.startAndGet();
                    });
                }
                else {
                    //result stage ------
                    checkState(stage instanceof ResultStage, "Unknown stage " + stage);
                    logger.info("starting... ResultStage: {}, stageId {}", stage, stage.getStageId());
                    Map<Integer, Integer> deps = stageMap.getOrDefault(stage, Collections.emptyMap());
                    return Stream.of(stage.getPartitions()).map(partition -> {
                        JVMLauncher<Serializable> jvmLauncher = JVMLaunchers.newJvm()
                                .setCallable(() -> {
                                    @SuppressWarnings("unchecked")
                                    ResultStage<E> resultStage = (ResultStage<E>) serializableStage.getValue();
                                    Operator<E> operator = resultStage.getFinalOperator();
                                    Iterator<E> iterator = operator.computeOrCache(partition,
                                            TaskContext.of(resultStage.getStageId(), deps));
                                    R r = action.map(iterator);
                                    return (Serializable) r;
                                })
                                .setConsole(System.out::println)
                                .build();
                        return (R) jvmLauncher.startAndGet();
                    }).collect(Collectors.toList());
                }
            }
        }
        finally {
            try {
                FileUtils.deleteDirectory(new File("/tmp/shuffle000"));
            }
            catch (IOException e) {
                logger.error("clear job tmp dir {} faild", "/tmp/shuffle");
            }
            finally {
                executors.shutdown();
            }
        }
        throw new UnsupportedOperationException("job " + jobId + " Not found ResultStage");
    }
}
