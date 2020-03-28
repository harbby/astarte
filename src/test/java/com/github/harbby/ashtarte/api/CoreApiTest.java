package com.github.harbby.ashtarte.api;

import com.github.harbby.ashtarte.MppContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CoreApiTest
{
    private final MppContext mppContext = MppContext.builder().getOrCreate();

    @Test
    public void fromCollectionTestReturnDataSet()
    {
        DataSet<String> dataSet = mppContext.makeDataSet(Arrays.asList("1", "2", "3"));
        Assert.assertNotNull(dataSet);
    }

    @Test
    public void fromCollectionTestPartitionSizeReturn4()
    {
        DataSet<String> dataSet = mppContext.makeDataSet(Arrays.asList("1", "2", "3"), 4);
        Partition[] partitions = dataSet.getPartitions();
        Assert.assertEquals(partitions.length, 4);
    }

    @Test
    public void fromArrayCollectTestReturn123()
    {
        DataSet<Integer> dataSet = mppContext.makeDataSet(1, 2, 3);
        List<Integer> collect = dataSet.collect();
        Assert.assertEquals(Arrays.asList(1, 2, 3), collect);
    }

    @Test
    public void fromCollectionTestMapReturnList123()
    {
        DataSet<String> dataSet = mppContext.makeDataSet(Arrays.asList("1", "2", "3"), 4);
        DataSet<Integer> mapDataSet = dataSet.map(k -> Integer.parseInt(k));
        List<Integer> mapList = mapDataSet.collect();
        Assert.assertEquals(Arrays.asList(1, 2, 3), mapList);
    }

    @Test
    public void cacheTest()
    {
        DataSet<String> dataSet = mppContext.makeDataSet(Arrays.asList("1", "2", "3"), 4);
        DataSet<String> rdd = dataSet.cache();
        List<String> r1 = rdd.collect();

        mppContext.makeDataSet(Arrays.asList("1", "2", "3"), 4)
                .cache().collect();
        System.out.println();
    }
}
