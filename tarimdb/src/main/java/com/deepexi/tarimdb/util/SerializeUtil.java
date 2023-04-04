package com.deepexi.tarimdb.util;

import java.io.*;

public class SerializeUtil {
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
}
