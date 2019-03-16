package codepig.ideal.mppwhater.operator;

import codepig.ideal.mppwhater.api.Partition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 存在设计问题，
 * 1. 因为每个Job会单独反序列化。缓存不能放到Operator中
 * 2. 并发job线程安全问题
 * */
public class CacheOperator<E>
        extends Operator<E>
{
    protected CacheOperator(Operator<?> oneParent)
    {
        super(oneParent);
    }

    @Override
    public Partition[] getPartitions()
    {
        return super.getPartitions();
    }

    private final List<E> cacheData = new ArrayList<>();
    private boolean cached = false;

    @Override
    public Iterator<E> compute(Partition split)
    {
        if (cached) {
            return cacheData.iterator();
        }
        else {
            Iterator<E> iterator = super.firstParent().compute(split);
            return new Iterator<E>()
            {
                @Override
                public boolean hasNext()
                {
                    boolean hasNext = iterator.hasNext();
                    if (!hasNext) {
                        cached = true;
                    }
                    return hasNext;
                }

                @Override
                public E next()
                {
                    E row = iterator.next();
                    cacheData.add(row);
                    return row;
                }

                @Override
                public void remove()
                {
                    iterator.remove();
                }
            };
        }
    }
}
