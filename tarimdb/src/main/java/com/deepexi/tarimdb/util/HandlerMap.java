package com.deepexi.tarimdb.util;

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.lang.System;

// not thread-safe
public class HandlerMap<V> 
{
    private Map<Long, V> handlers;

    public HandlerMap()
    {
        handlers = new HashMap<>();
    }

    private long getUnique()
    {
        long ts = System.currentTimeMillis();
        do{
            if(handlers.containsKey(ts))
            {
                ts++;
            }else{
                return ts;
            }
        }while(true);
    }

    public long put(V v)
    {
        long key = getUnique();
        handlers.put(new Long(key), v);
        return key;
    }

    public V get(long handler)
    {
        return handlers.get(new Long(handler));
    }

    public void remove(long handler)
    {
        handlers.remove(handler);
    }

    // ttl: seconds
    // return 0: means not found
    public long getEarliestHandler(long ttl)
    {
        Long maxKey = Collections.min(handlers.keySet());
        long tsNow = System.currentTimeMillis();
        if(tsNow - maxKey >= ttl * 1000) return maxKey.longValue();
        else return 0;
    }
}

