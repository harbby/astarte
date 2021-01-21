package com.github.harbby.astarte.operator;

import com.github.harbby.astarte.BatchContext;
import com.github.harbby.astarte.TaskContext;
import com.github.harbby.astarte.api.Partition;

import com.github.harbby.gadtry.collection.tuple.Tuple2;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class CollectionSource<E>
        extends Operator<E> {
    private final int parallelism;
    private final Collection<E> collection;

    public CollectionSource(BatchContext context, Collection<E> collection, int parallelism) {
        super(context);
        this.collection = collection;
        this.parallelism = parallelism;
    }

    @Override
    public Partition[] getPartitions() {
        return slice(collection, parallelism);
    }

    @Override
    public Iterator<E> compute(Partition partition, TaskContext taskContext) {
        ParallelCollectionPartition<E> collectionPartition = (ParallelCollectionPartition<E>) partition;
        return collectionPartition.getCollection().iterator();
    }

    private static <E> Partition[] slice(Collection<E> collection, int parallelism) {
        long length = collection.size();
        List<Tuple2<Integer, Integer>> tuple2s = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            int start = (int) ((i * length) / parallelism);
            int end = (int) (((i + 1) * length) / parallelism);
            tuple2s.add(Tuple2.of(start, end));
        }

        List<E> list = ImmutableList.copyOf(collection);
        ParallelCollectionPartition[] partitions = new ParallelCollectionPartition[tuple2s.size()];
        for (int i = 0; i < partitions.length; i++) {
            Tuple2<Integer, Integer> a1 = tuple2s.get(i);
            partitions[i] = new ParallelCollectionPartition<>(i, list.subList(a1.f1(), a1.f2()));
        }
        return partitions;
    }

    private static class ParallelCollectionPartition<ROW>
            extends Partition {
        private final Collection<ROW> collection;

        public ParallelCollectionPartition(int index, Collection<ROW> collection) {
            super(index);
            this.collection = collection;
        }

        public Collection<ROW> getCollection() {
            return collection;
        }
    }
}
