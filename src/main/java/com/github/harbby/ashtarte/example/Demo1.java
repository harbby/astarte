package com.github.harbby.ashtarte.example;

import com.github.harbby.ashtarte.MppContext;
import com.github.harbby.ashtarte.api.DataSet;

import java.util.Arrays;

public class Demo1
{
    public static void main(String[] args)
    {
        MppContext mppContext = MppContext.builder().getOrCreate();
        DataSet<String> dataSet = mppContext.makeDataSet(Arrays.asList("1", "2", "3"));

        DataSet<Integer> a1 = dataSet.map(k -> {
            return Integer.parseInt(k);
        }).map(k -> k + 1);

        a1.foreach(x -> {
            System.out.println(x);
        });
    }
}
