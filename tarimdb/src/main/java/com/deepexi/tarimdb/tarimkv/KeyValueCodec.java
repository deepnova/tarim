package com.deepexi.tarimdb.tarimkv;

import java.util.Map;
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
    public TarimKVProto.KeyValue value;
    public TarimKVProto.KeyValueOp valueOp;

    public KeyValueCodec(){ }

    public KeyValueCodec(long chunkID, TarimKVProto.KeyValue value)
    {
        this.chunkID = chunkID;
        this.value = value;
        logger.debug("KeyValueCodec(), chunkID: " + this.chunkID 
                  + ", key: " + this.value.getKey());
    }

    // Key固定编码：{chunkID}{separator}{primaryKey}
    public static String KeyEncode(KeyValueCodec kv) 
    {
        String internalKey = String.format("%d%s%s"
                                          ,kv.chunkID
                                          ,KeyValueCodec.KEY_SEPARATOR
                                          ,kv.value.getKey());
        logger.debug("KeyEncode(), chunkID: " + kv.chunkID 
                  + ", key: " + kv.value.getKey()
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
        String internalKey = String.format("%d%s%s"
                                          ,chunkID
                                          ,KeyValueCodec.KEY_SEPARATOR
                                          ,prefix);
        logger.debug("KeyPrefixEncode(), chunkID: " + chunkID 
                  + ", prefix: " + prefix
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

    public static KeyValueCodec KeyDecode(String internalKey, String value) throws TarimKVException
    {
        String[] results = internalKey.split(KeyValueCodec.KEY_SEPARATOR);
        //logger.info("KeyDecode(), internalKey: " + internalKey
        //          + ", results: " + results.toString());
        if(results.length != 2) return null; //throw new TarimKVException(Status.KEY_ENCODE_ERROR);
        KeyValueCodec kvc = new KeyValueCodec();
        kvc.chunkID = Long.parseLong(results[0]);
        TarimKVProto.KeyValue.Builder kvBuilder = TarimKVProto.KeyValue.newBuilder();
        kvBuilder.setKey(results[1]);
        kvBuilder.setValue(value);
        kvc.value = kvBuilder.build();
        return kvc;
    }

    public static KeyValueCodec OpKeyDecode(String internalKey) throws TarimKVException
    {
        String[] result = internalKey.split(KeyValueCodec.KEY_SEPARATOR);
        if(!(result.length == 2 || result.length == 3)) return null; //throw new TarimKVException(Status.KEY_ENCODE_ERROR);
        KeyValueCodec kvc = new KeyValueCodec();
        kvc.chunkID = Long.parseLong(result[0]);
        TarimKVProto.KeyValueOp.Builder kvBuilder = TarimKVProto.KeyValueOp.newBuilder();
        kvBuilder.setKey(result[1]);
        if(result.length == 3)
        {
            if(result[2].equals("new")) kvBuilder.setOp(1);
            else if(result[2].equals("del")) kvBuilder.setOp(2);
            else kvBuilder.setOp(0); //TODO: error
        }
        kvc.valueOp = kvBuilder.build();
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
}


