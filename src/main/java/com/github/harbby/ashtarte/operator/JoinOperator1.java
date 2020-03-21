package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.TaskContext;
import com.github.harbby.ashtarte.api.Partition;
import com.github.harbby.ashtarte.api.ShuffleManager;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.github.harbby.gadtry.base.MoreObjects.checkState;

public class JoinOperator1<K, V, W>
        //extends KvOperator<K, Tuple2<V, W>>
        extends Operator<Tuple2<K, Tuple2<V, W>>>
{

    ShuffleMapOperator<K, V> shuffleMapper1;
    ShuffleMapOperator<K, W> shuffleMapper2;

    protected JoinOperator1(
            ShuffleMapOperator<K, V> shuffleMapper1,
            ShuffleMapOperator<K, W> shuffleMapper2)
    {
        super(shuffleMapper1, shuffleMapper2);
        this.shuffleMapper1 = shuffleMapper1;
        this.shuffleMapper2 = shuffleMapper2;
        checkState(shuffleMapper1.getPartitioner() == shuffleMapper2.getPartitioner());
    }

    @Override
    public Partition[] getPartitions()
    {
        return IntStream.range(0, shuffleMapper1.getPartitioner().numPartitions())
                .mapToObj(Partition::new).toArray(Partition[]::new);
    }

    /**
     * 最难算子...
     */
    @Override
    public Iterator<Tuple2<K, Tuple2<V, W>>> compute(Partition split, TaskContext taskContext)
    {
        int shuffleId = taskContext.getStageId() - 1;

        Iterator<Tuple2<K, W>> iterator2 = ShuffleManager.getReader(shuffleId, split.getId());
        Iterator<Tuple2<K, V>> iterator1 = ShuffleManager.getReader(shuffleId - 1, split.getId());

        Map<K, Tuple2<List<V>, List<W>>> map = new HashMap<>();
        while (iterator1.hasNext()) {
            Tuple2<K, V> t = iterator1.next();
            Tuple2<List<V>, List<W>> values = map.get(t.f1());
            if (values == null) {
                values = new Tuple2<>(new ArrayList<>(), new ArrayList<>());
                map.put(t.f1(), values);
            }
            values.f1().add(t.f2());
        }

        while (iterator2.hasNext()) {
            Tuple2<K, W> t = iterator2.next();
            Tuple2<List<V>, List<W>> values = map.get(t.f1());
            if (values == null) {
                values = new Tuple2<>(new ArrayList<>(), new ArrayList<>());
                map.put(t.f1(), values);
            }
            values.f2().add(t.f2());
        }
        return map.entrySet().stream()
                .filter(x -> !x.getValue().f2().isEmpty() && !x.getValue().f1().isEmpty())
                .flatMap(x -> {
                    Tuple2<List<V>, List<W>> values = x.getValue();
                    return values.f1().stream().flatMap(x2 -> values.f2().stream().map(x3 -> new Tuple2<>(x2, x3)))
                            .map(x3 -> new Tuple2<>(x.getKey(), x3));
                }).iterator();
    }
}
