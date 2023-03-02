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
                  + ", key: " + this.value.getKey()
                  + ", encodeVersion: " + this.value.getEncodeVersion());
    }

    // Key固定编码：{chunkID}{separator}{primaryKey}{separator}{encodeVersion}
    public static String KeyEncode(KeyValueCodec kv) 
    {
        String internalKey = String.format("%ld%s%s%s%d"
                                          ,kv.chunkID
                                          ,KeyValueCodec.KEY_SEPARATOR
                                          ,kv.value.getKey()
                                          ,KeyValueCodec.KEY_SEPARATOR
                                          ,kv.value.getEncodeVersion());
        logger.debug("KeyEncode(), chunkID: " + kv.chunkID 
                  + ", key: " + kv.value.getKey()
                  + ", encodeVersion: " + kv.value.getEncodeVersion() + ", internalKey: " + internalKey);
        return internalKey;
    }

    public static String KeyPrefixEncode(long chunkID, String prefix) 
    {
        String internalKey = String.format("%ld%s%s%s%d"
                                          ,chunkID
                                          ,KeyValueCodec.KEY_SEPARATOR
                                          ,prefix
                                          ,KeyValueCodec.KEY_SEPARATOR);
        logger.debug("KeyPrefixEncode(), chunkID: " + chunkID 
                  + ", prefix: " + prefix
                  + ", internalKey prefix: " + internalKey);
        return internalKey;
    }

    public static String ChunkOnlyKeyPrefixEncode(long chunkID) 
    {
        String internalKey = String.format("%ld%s"
                                          ,chunkID
                                          ,KeyValueCodec.KEY_SEPARATOR);
        logger.debug("ChunkOnlyKeyPrefixEncode(), chunkID: " + chunkID 
                  + ", internalKey prefix: " + internalKey);
        return internalKey;
    }

    public static KeyValueCodec KeyDecode(String internalKey) throws TarimKVException
    {
        String[] result = internalKey.split(KeyValueCodec.KEY_SEPARATOR);
        if(result.length != 3) throw new TarimKVException(Status.KEY_ENCODE_ERROR);
        KeyValueCodec kvc = new KeyValueCodec();
        kvc.chunkID = Long.parseLong(result[0]);
        TarimKVProto.KeyValue.Builder kvBuilder = TarimKVProto.KeyValue.newBuilder();
        kvBuilder.setKey(result[1]);
        kvBuilder.setEncodeVersion(Integer.parseInt(result[2]));
        kvc.value = kvBuilder.build();
        return kvc;
    }

    public static KeyValueCodec OpKeyDecode(String internalKey) throws TarimKVException
    {
        String[] result = internalKey.split(KeyValueCodec.KEY_SEPARATOR);
        if(!(result.length == 3 || result.length == 4)) throw new TarimKVException(Status.KEY_ENCODE_ERROR);
        KeyValueCodec kvc = new KeyValueCodec();
        kvc.chunkID = Long.parseLong(result[0]);
        TarimKVProto.KeyValueOp.Builder kvBuilder = TarimKVProto.KeyValueOp.newBuilder();
        kvBuilder.setKey(result[1]);
        kvBuilder.setEncodeVersion(Integer.parseInt(result[2]));
        if(result.length == 4)
        {
            if(result[3].equals("new")) kvBuilder.setOp(1);
            else if(result[3].equals("del")) kvBuilder.setOp(2);
            else kvBuilder.setOp(0); //TODO: error
        }
        kvc.valueOp = kvBuilder.build();
        return kvc;
    }

    public static void putMaxVersionKeyValue(TarimKVProto.KeyValue kvSrc, Map<String, TarimKVProto.KeyValue> mapDest)
    {
        //TODO: There should not be two values for a key with different encodeVersions
        TarimKVProto.KeyValue kv = mapDest.get(kvSrc.getKey());
        if(kv == null || kvSrc.getEncodeVersion() > kv.getEncodeVersion())
        {
            mapDest.put(kvSrc.getKey(), kvSrc);
        }
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


