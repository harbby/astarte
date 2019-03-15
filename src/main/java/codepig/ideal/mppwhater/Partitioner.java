package codepig.ideal.mppwhater;

/**
 * numReduceTasks = numPartitions
 */
public abstract class Partitioner<KEY>
{
    public abstract int getPartition(KEY key, int numReduceTasks);
}
