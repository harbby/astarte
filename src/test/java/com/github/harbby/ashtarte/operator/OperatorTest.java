package com.github.harbby.ashtarte.operator;

import com.github.harbby.ashtarte.MppContext;
import com.github.harbby.ashtarte.api.DataSet;
import com.github.harbby.ashtarte.api.KvDataSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class OperatorTest
{
    private final MppContext mppContext = MppContext.builder()
            .setParallelism(2)
            .getOrCreate();

    @Test
    public void zipWithIndex()
    {
        DataSet<String> links = mppContext.makeDataSet(Arrays.asList("a",
                "b",
                "c",
                "d",
                "e"), 2);

        KvDataSet<String, Long> zipIndex = links.zipWithIndex();
        List<Long> indexs = zipIndex.values().collect();
        Assert.assertEquals(Arrays.asList(0L, 1L, 2L, 3L, 4L), indexs);
    }
}