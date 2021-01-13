package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.BatchContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.gadtry.collection.MutableMap;
import com.github.harbby.gadtry.collection.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class KeyValueGroupedOperatorTest
{
    private final BatchContext mppContext = BatchContext.builder()
            .setParallelism(2)
            .getOrCreate();

    @Test
    public void keyGroupedTest()
    {
        DataSet<String> ds = mppContext.makeDataSet(Arrays.asList(
                "a",
                "a",
                "b",
                "b",
                "b"), 2);

        Map<Integer, String> result = ds.groupByKey(x -> x.charAt(0) % 2)
                .<StringBuilder>partitionGroupsWithState(keyGroupState -> (record) -> {
                    if (keyGroupState.getState() == null) {
                        keyGroupState.update(new StringBuilder(keyGroupState.getKey()));
                    }
                    StringBuilder builder = keyGroupState.getState();
                    builder.append(record);
                }).collect().stream().collect(Collectors.toMap(k -> k.f1, v -> v.f2.toString()));
        Assert.assertEquals(result, MutableMap.of(0, "bbb", 1, "aa"));
    }

    @Test
    public void keyByMapPartitionTest()
            throws IOException
    {
        DataSet<String> ds = mppContext.makeDataSet(Arrays.asList(
                "a",
                "a",
                "b",
                "b",
                "b"), 1);

        Map<Integer, String> result = ds.groupByKey(x -> x.charAt(0) % 2)
                .mapPartition(partition -> {
                    Map<Integer, StringBuilder> keyGroup = new HashMap<>();
                    while (partition.hasNext()) {
                        Tuple2<Integer, String> row = partition.next();
                        keyGroup.computeIfAbsent(row.f1, k -> new StringBuilder()).append(row.f2);
                    }
                    return keyGroup.entrySet().stream().map(x -> new Tuple2<>(x.getKey(), x.getValue())).iterator();
                }).collect().stream().collect(Collectors.toMap(k -> k.getKey(), v -> v.getValue().toString()));
        Assert.assertEquals(result, MutableMap.of(0, "bbb", 1, "aa"));
    }
}