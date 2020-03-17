package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.Partitioner;
import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.ShuffleManager;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class JoinOperator<K>
        //extends KvOperator<K, Tuple2<V, W>>
        extends Operator<Tuple2<K, Iterable<?>[]>>
{
    private final DataSet<? extends Tuple2<K, ?>>[] kvDataSets;
    private final Partitioner<K> partitioner;

    @SafeVarargs
    protected JoinOperator(Partitioner<K> partitioner, DataSet<? extends Tuple2<K, ?>>... kvDataSets)
    {
        super(kvDataSets[0].getContext());
        this.kvDataSets = kvDataSets;
        this.partitioner = partitioner;
    }

    @Override
    public List<Operator<?>> getDependencies()
    {
        List<Operator<?>> deps = new ArrayList<>(kvDataSets.length);
        for (DataSet<? extends Tuple2<K, ?>> dataSet : kvDataSets) {
            Operator<Tuple2<K, Object>> operator = (Operator<Tuple2<K, Object>>) dataSet;
            deps.add(new ShuffleMapOperator<>(operator, partitioner));
        }
        return deps;
    }

    @Override
    public Partition[] getPartitions()
    {
        return IntStream.range(0, partitioner.numPartitions())
                .mapToObj(Partition::new).toArray(Partition[]::new);
    }

    /**
     * 最难算子...
     */
    @Override
    public Iterator<Tuple2<K, Iterable<?>[]>> compute(Partition split, TaskContext taskContext)
    {
        Map<K, Iterable<?>[]> map = new HashMap<>();
        int[] deps = taskContext.getDependStages();

        for (int i = 0; i < deps.length; i++) {
            int shuffleId = deps[i];
            Iterator<Tuple2<K, Object>> iterator = ShuffleManager.getReader(shuffleId, split.getId());

            while (iterator.hasNext()) {
                Tuple2<K, Object> t = iterator.next();
                Iterable<?>[] values = map.get(t.f1());
                if (values == null) {
                    values = new Iterable[deps.length];
                    for (int j = 0; j < deps.length; j++) {
                        values[j] = new ArrayList<>();
                    }
                    map.put(t.f1(), values);
                }
                ((List<Object>) values[i]).add(t.f2());
            }
        }

        return map.entrySet().stream().map(x -> new Tuple2<>(x.getKey(), x.getValue()))
                .iterator();
    }
}
