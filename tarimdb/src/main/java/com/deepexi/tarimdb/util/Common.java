package com.deepexi.tarimdb.util;

import com.deepexi.tarimdb.tarimkv.YamlLoader;
import org.apache.iceberg.StructLike;

import java.io.InputStream;
import java.util.*;

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

    public static String loadTableMeta(String fileName){
        InputStream inputStream = YamlLoader.class
                .getClassLoader()
                .getResourceAsStream(fileName);

        String jsonString = null;

        try (Scanner scanner = new Scanner(inputStream)) {
            jsonString = scanner.useDelimiter("\\A").next();
        }

        return jsonString;
    }

    public static class Row implements StructLike {
        public static Row of(Object... values) {
            return new Row(values);
        }

        private final Object[] values;

        private Row(Object... values) {
            this.values = values;
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T get(int pos, Class<T> javaClass) {
            return javaClass.cast(values[pos]);
        }

        @Override
        public <T> void set(int pos, T value) {
            throw new UnsupportedOperationException("Setting values is not supported");
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            Row that = (Row) other;

            return Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}

