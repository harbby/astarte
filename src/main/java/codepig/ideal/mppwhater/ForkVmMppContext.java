package codepig.ideal.mppwhater;

import codepig.ideal.mppwhater.api.Partition;
import codepig.ideal.mppwhater.api.function.Foreach;
import codepig.ideal.mppwhater.api.operator.AbstractDataSet;
import com.github.harbby.gadtry.jvm.JVMException;
import com.github.harbby.gadtry.jvm.JVMLauncher;
import com.github.harbby.gadtry.jvm.JVMLaunchers;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class ForkVmMppContext
        implements MppContext
{
    @Override
    public <E> List<E> collect(AbstractDataSet<E> dataSet)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    @Override
    public <E> void execJob(AbstractDataSet<E> dataSet, Foreach<Iterator<E>> partitionForeach)
    {
        Partition[] partitions = dataSet.getPartitions();
        Stream.of(partitions).parallel().forEach(partition -> {
                    JVMLauncher<String> jvmLauncher = JVMLaunchers.<String>newJvm()
                            .setCallable(() -> {
                                Iterator<E> iterator = dataSet.compute(partition);
                                partitionForeach.apply(iterator);
                                return "";
                            })
                            .setConsole(System.out::println)
                            .build();
                    try {
                        jvmLauncher.startAndGet();
                    }
                    catch (JVMException e) {
                        e.printStackTrace();
                    }
                }
        );
    }
}
