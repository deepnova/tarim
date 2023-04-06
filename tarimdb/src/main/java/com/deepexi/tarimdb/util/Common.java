package com.deepexi.tarimdb.util;

import com.deepexi.tarimdb.tarimkv.YamlLoader;
import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.iceberg.StructLike;

import java.io.*;
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

    public static byte[] serialize(Object object) {
        ObjectOutputStream oos = null;
        ByteArrayOutputStream bos = null;
        try {
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(object);
            byte[] b = bos.toByteArray();
            return b;
        } catch (IOException e) {
            System.out.println("serialize Exception:" + e.toString());
            return null;
        } finally {
            try {
                if (oos != null) {
                    oos.close();
                }
                if (bos != null) {
                    bos.close();
                }
            } catch (IOException ex) {
                System.out.println("io could not close:" + ex.toString());
            }
        }
    }


    public static Object deserialize(byte[] bytes) {
        ByteArrayInputStream bis = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("bytes Could not deserialize:" + e.toString());
            return null;
        } finally {
            try {
                if (bis != null) {
                    bis.close();
                }
            } catch (IOException ex) {
                System.out.println("LogManage Could not serialize:" + ex.toString());
            }
        }
    }

    public static Long toChunkID(String partitionID){
        if (partitionID == null){
            throw new RuntimeException("partitionID is invalid!");
        }
        return partitionID.hashCode() & 0x00000000FFFFFFFFL;
    }

    public static Long chunkIDHash(long chunkID){
        return MurmurHash3.hash32(chunkID) & 0x00000000FFFFFFFFL;
    }
    public static Long toHashCode(String partitionID){
        long chunkID = toChunkID(partitionID);
        long hash = chunkIDHash(chunkID);

        return hash;
    }
}

