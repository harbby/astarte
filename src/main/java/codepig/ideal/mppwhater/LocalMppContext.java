package codepig.ideal.mppwhater;

import codepig.ideal.mppwhater.api.Stage;
import codepig.ideal.mppwhater.operator.Operator;
import codepig.ideal.mppwhater.operator.ResultStage;
import codepig.ideal.mppwhater.operator.ShuffleMapStage;
import codepig.ideal.mppwhater.operator.ShuffleOperator;
import codepig.ideal.mppwhater.utils.SerializableObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Local achieve
 */
public class LocalMppContext
        implements MppContext
{
    private static final Logger logger = LoggerFactory.getLogger(LocalMppContext.class);
    private static final AtomicInteger nextJobId = new AtomicInteger(1);  //发号器

    private static <E> ResultStage<E> startStage(Operator<E> dataSet)
    {
        List<Stage> stages = new ArrayList<>();
        for (Operator<?> parent = dataSet; parent != null; parent = parent.firstParent()) {
            if (parent instanceof ShuffleOperator) {
                stages.add(0, new ShuffleMapStage(parent));
            }
        }

        for (Stage stage : stages) {
            logger.info("starting... stage: {}", stage);
            SerializableObj<Stage> serializableStage = SerializableObj.of(stage);
            ExecutorService executors = Executors.newFixedThreadPool(stage.getParallel());
            try {
                Stream.of(stage.getPartitions()).map(partition -> CompletableFuture.runAsync(() -> {
                    Stage operator = serializableStage.getValue();
                    operator.compute(partition);
                }, executors)).collect(Collectors.toList()).forEach(x -> x.join());
            }
            finally {
                executors.shutdown();
            }
        }

        return new ResultStage<>(dataSet);   //最后一个state
    }

    @Override
    public <E, R> List<R> runJob(Operator<E> dataSet, Function<Iterator<E>, R> function)
    {
        int jobId = nextJobId.getAndIncrement();
        logger.info("starting... job: {}", jobId);
        ResultStage<E> resultStage = startStage(dataSet);

        SerializableObj<Operator<E>> serializableObj = SerializableObj.of(dataSet);
        ExecutorService executors = Executors.newFixedThreadPool(resultStage.getParallel());
        try {
            return Stream.of(resultStage.getPartitions()).map(partition -> CompletableFuture.supplyAsync(() -> {
                Operator<E> operator = serializableObj.getValue();
                Iterator<E> iterator = operator.compute(partition);
                return function.apply(iterator);
            }, executors)).collect(Collectors.toList()).stream()
                    .map(x -> x.join())
                    .collect(Collectors.toList());
        }
        finally {
            executors.shutdown();
        }
    }
}
