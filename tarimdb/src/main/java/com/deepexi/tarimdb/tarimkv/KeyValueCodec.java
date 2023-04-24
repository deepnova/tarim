package com.deepexi.tarimdb.tarimkv;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.rpc.TarimKVProto;
import com.deepexi.tarimdb.util.Status;
import com.deepexi.tarimdb.util.TarimKVException;

/**
 * KeyValueCodec 
 *  
 */
public class KeyValueCodec 
{
    private final static Logger logger = LogManager.getLogger(KeyValueCodec.class);

    private static String KEY_SEPARATOR = "_";

    public long chunkID;

    public int tableID;
    public TarimKVProto.KeyValue valueKV;
    public TarimKVProto.KeyValueByte value;
    public TarimKVProto.KeyValueOp valueOp;

    public KeyValueCodec(){ }

    public KeyValueCodec(long chunkID, TarimKVProto.KeyValueByte value)
    {
        this.chunkID = chunkID;
        this.value = value;
        logger.debug("KeyValueCodec(), chunkID: " + this.chunkID 
                  + ", key: " + this.value.getKey());
    }

    public KeyValueCodec(int tableID, TarimKVProto.KeyValueByte value)
    {
        this.tableID = tableID;
        this.value = value;
        logger.debug("KeyValueCodec(), tableID: " + this.tableID
                + ", key: " + this.value.getKey());
    }

    // Key固定编码：{chunkID}{separator}{primaryKey}
    public static String KeyEncode(KeyValueCodec kv) 
    {
        //todo get the Type and convert  the primaryKey
        Long key = Long.parseLong(kv.value.getKey());

        String internalKey = String.format("%d%s%s"
                                          ,kv.chunkID
                                          ,KeyValueCodec.KEY_SEPARATOR
                                          ,keyLongBase16Codec(key));
        logger.debug("KeyEncode(), chunkID: " + kv.chunkID 
                  + ", key: " + Arrays.toString(keyLongBase16Codec(key).getBytes())
                  + ", internalKey: " + internalKey);
        return internalKey;
    }

    public static String KeyEncode(long chunkID, String key) 
    {
        String internalKey = String.format("%d%s%s"
                                          ,chunkID
                                          ,KeyValueCodec.KEY_SEPARATOR
                                          ,key);
        logger.debug("KeyEncode(), chunkID: " + chunkID 
                  + ", key: " + key + ", internalKey: " + internalKey);
        return internalKey;
    }

    public static String KeyPrefixEncode(long chunkID, String prefix) 
    {
        //todo get the Type and convert  the primaryKey
        Long key = Long.parseLong(prefix);

        String internalKey = String.format("%d%s%s"
                                          ,chunkID
                                          ,KeyValueCodec.KEY_SEPARATOR
                                          ,keyLongBase16Codec(key));
        logger.debug("KeyPrefixEncode(), chunkID: " + chunkID 
                  + ", prefix: " + Arrays.toString(keyLongBase16Codec(key).getBytes())
                  + ", internalKey prefix: " + internalKey);
        return internalKey;
    }

    public static String ChunkOnlyKeyPrefixEncode(long chunkID) 
    {
        String internalKey = String.format("%d%s"
                                          ,chunkID
                                          ,KeyValueCodec.KEY_SEPARATOR);
        logger.debug("ChunkOnlyKeyPrefixEncode(), chunkID: " + chunkID 
                  + ", internalKey prefix: " + internalKey);
        return internalKey;
    }

    public static KeyValueCodec  KeyDecode(String internalKey, byte[] value) throws TarimKVException
    {
        String[] results = internalKey.split(KeyValueCodec.KEY_SEPARATOR);
        //logger.info("KeyDecode(), internalKey: " + internalKey
        //          + ", results: " + results.toString());
        if(results.length != 2) return null; //throw new TarimKVException(Status.KEY_ENCODE_ERROR);
        KeyValueCodec kvc = new KeyValueCodec();
        kvc.chunkID = Long.parseLong(results[0]);
        TarimKVProto.KeyValueByte.Builder kvBuilder = TarimKVProto.KeyValueByte.newBuilder();
        kvBuilder.setKey(results[1]);
        kvBuilder.setValue(ByteString.copyFrom(value));
        kvc.value = kvBuilder.build();
        return kvc;
    }

    public static KeyValueCodec OpKeyDecode(String internalKey, byte[] value) throws TarimKVException
    {
        String[] result = internalKey.split(KeyValueCodec.KEY_SEPARATOR);
        //support op=1 first,
        if(!(result.length == 2 || result.length == 3)) return null; //throw new TarimKVException(Status.KEY_ENCODE_ERROR);
        KeyValueCodec kvc = new KeyValueCodec();
        kvc.chunkID = Long.parseLong(result[0]);
        TarimKVProto.KeyValueOp.Builder kvBuilder = TarimKVProto.KeyValueOp.newBuilder();
        kvBuilder.setKey(result[1]);
        kvBuilder.setValue(ByteString.copyFrom(value));

        if(result.length == 3)
        {
            if(result[2].equals("new")) kvBuilder.setOp(1);
            else if(result[2].equals("del")) kvBuilder.setOp(2);
            else kvBuilder.setOp(0); //TODO: error
        }

        kvBuilder.setOp(1);
        kvc.valueOp = kvBuilder.build();
        return kvc;
    }

    public static String schemaKeyEncode(int tableID, String key)
    {
        //check
        String internalKey = String.format("%d%s%s"
                ,tableID
                ,KeyValueCodec.KEY_SEPARATOR
                ,key);
        logger.debug("partitionKeyEncode(), tableID: " + tableID
                + ", key: " + key
                + ", internalKey : " + internalKey);
        return internalKey;
    }

    public static KeyValueCodec schemaKeyDecode(String internalKey, String value) throws TarimKVException
    {
        String[] results = internalKey.split(KeyValueCodec.KEY_SEPARATOR);
        //logger.info("KeyDecode(), internalKey: " + internalKey
        //          + ", results: " + results.toString());
        if(results.length != 2) return null; //throw new TarimKVException(Status.KEY_ENCODE_ERROR);
        KeyValueCodec kvc = new KeyValueCodec();
        kvc.tableID = Integer.parseInt(results[0]);
        TarimKVProto.KeyValue.Builder kvBuilder = TarimKVProto.KeyValue.newBuilder();
        kvBuilder.setKey(results[1]);
        kvBuilder.setValue(value);
        kvc.valueKV = kvBuilder.build();
        return kvc;
    }

    public static byte[] ValueEncode() 
    {
        // un-used
        return null;
    }
    public static byte[] ValueDecode() 
    {
        // un-used
        return null;
    }

    public static String keyLongBase16Codec(long value) {
        value = value ^ 0x8000000000000000L;

        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(value);
        byte[] bytes = buffer.array();

        char[] chars = new char[bytes.length];
        logger.info("after codec: " + Arrays.toString(bytes));
        for (int i = 0; i < bytes.length; i++) {
            chars[i] = (char) bytes[i];
        }


        return new String(chars);
    }

    public static String keyIntBase16Codec(int value) {
        value = value ^ 0x80000000;

        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);
        byte[] bytes = buffer.array();

        char[] chars = new char[bytes.length];
        logger.info("after codec: " + Arrays.toString(bytes));
        for (int i = 0; i < bytes.length; i++) {
            chars[i] = (char) bytes[i];
        }

        return new String(chars);
    }

}


