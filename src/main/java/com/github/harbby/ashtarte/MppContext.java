package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.KvDataSet;
import com.github.harbby.ashtarte.operator.CollectionDataSet;
import com.github.harbby.ashtarte.operator.KvOperator;
import com.github.harbby.ashtarte.operator.Operator;
import com.github.harbby.ashtarte.operator.TextFileDataSet;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public interface MppContext {

    public default <K, V> KvDataSet<K, V> makeKvDataSet(Collection<Tuple2<K, V>> collection, int parallelism) {
        return new KvOperator<>(new CollectionDataSet<>(this, collection, parallelism));
    }

    public default <K, V> KvDataSet<K, V> makeKvDataSet(Collection<Tuple2<K, V>> collection) {
        return new KvOperator<>(new CollectionDataSet<>(this, collection, 2));
    }

    public default <E> DataSet<E> fromCollection(Collection<E> collection) {
        return new CollectionDataSet<>(this, collection, 2);
    }

    public default <E> DataSet<E> fromCollection(Collection<E> collection, int parallelism) {
        return new CollectionDataSet<>(this, collection, parallelism);
    }

    public default <E> DataSet<E> fromArray(E... e) {
        return fromCollection(Arrays.asList(e), 2);
    }

    public default DataSet<String> textFile(String dirPath) {
        return new TextFileDataSet(this, dirPath);
    }

    public void setParallelism(int parallelism);

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final MppContext context = new LocalMppContext();

        public Builder setParallelism(int parallelism) {
            context.setParallelism(parallelism);
            return this;
        }

        public MppContext getOrCreate() {
            MppContext context = new LocalMppContext();
            return context;
        }
    }

    public <E, R> List<R> runJob(Operator<E> dataSet, Function<Iterator<E>, R> action);
}
