package com.github.harbby.ashtarte.operator;

//import com.google.common.collect.MapMaker;

import java.util.List;

/**
 * cache
 */
@Deprecated
public class CacheManager {
    //private static final ConcurrentMap<Integer, List<?>> cacheMap = new MapMaker().weakValues().makeMap();

    public static void addCache(int id, List<?> data) {
        throw new UnsupportedOperationException();
    }

    public static List getCacheData(int id) {
        throw new UnsupportedOperationException();
    }
}
