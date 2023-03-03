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

    public static String BytesListToString(List<byte[]> bytesList)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for(byte[] str : bytesList)
        {
            if(str == null) sb.append("(null)");
            else sb.append(new String(str));
            sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    public static boolean startWith(final byte[] src, final byte[] prefix)
    {
        if(src.length < prefix.length) return false;
        for(int i = 0; i < prefix.length; i++)
        {
            if(src[i] != prefix[i]) return false;
        }
        return true;
    }
}

