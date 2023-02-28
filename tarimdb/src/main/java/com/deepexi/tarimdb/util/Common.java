package com.deepexi.tarimdb.util;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Common {

    public static<T> Iterable<T> iteratorToIterable(Iterator<T> iterator)
    {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return iterator;
            }
        };
    }

    public static List<byte[]> stringListToBytesList(List<String> strList)
    {
        List<byte[]> bytesList = new ArrayList<>();
        for(String str : strList)
        {
            bytesList.add(str.getBytes());
        }
        return bytesList;
    }

}

