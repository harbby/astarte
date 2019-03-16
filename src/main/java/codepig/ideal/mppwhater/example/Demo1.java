package codepig.ideal.mppwhater.example;

import codepig.ideal.mppwhater.MppContext;
import codepig.ideal.mppwhater.api.DataSet;

import java.util.Arrays;

public class Demo1
{
    public static void main(String[] args)
    {
        MppContext mppContext = MppContext.getOrCreate();
        DataSet<String> dataSet = mppContext.fromCollection(Arrays.asList("1", "2", "3"));

        DataSet<Integer> a1 = dataSet.map(k -> {
            return Integer.parseInt(k);
        }).map(k -> k + 1);

        a1.foreach(x -> {
            System.out.println(x);
        });
    }
}
