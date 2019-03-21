package codepig.ideal.mppwhater.operator;

import codepig.ideal.mppwhater.api.Partition;
import com.github.harbby.gadtry.collection.tuple.Tuple2;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * mapping映射 shuffle过程通过网络发送数据
 */
public class ShufflePartition<KEY, VALUE>
        extends Partition
        implements AutoCloseable
{
    private final int partitionId;

    public ShufflePartition(int id, int index)
    {
        super(index);
        this.partitionId = id * 1000 + index;
        // 根据index 和具体 worker建立连接
        CacheManager.reduceTaskMap.put(partitionId, new LinkedBlockingQueue<>());
    }

    public void send(KEY key, VALUE row)
    {
        Queue<Tuple2<KEY, VALUE>> buffer = (Queue<Tuple2<KEY, VALUE>>) CacheManager.reduceTaskMap.get(partitionId);
        buffer.add(Tuple2.of(key, row));
    }

    public Queue<Tuple2<KEY, VALUE>> getBuffer()
    {
        return (Queue<Tuple2<KEY, VALUE>>) CacheManager.reduceTaskMap.get(partitionId);
    }

    @Override
    public void close()
            throws Exception
    {
        //关闭连接
        Queue list = CacheManager.reduceTaskMap.get(partitionId);
        if (list != null) {
            list.clear();
        }
    }
}
