package com.deepexi.tarimdb.tarimkv;

import com.deepexi.rpc.TarimKVProto;
import com.deepexi.tarimdb.util.Status;
import com.deepexi.tarimdb.util.TarimKVException;

/**
 * KeyValueCodec 
 *  
 */
public class KeyValueCodec {
    private static String KEY_SEPARATOR = "_";

    public int chunkID;
    public TarimKVProto.KeyValue value;

    public KeyValueCodec(){ }

    public KeyValueCodec(int chunkID, TarimKVProto.KeyValue value){
        this.chunkID = chunkID;
        this.value = value;
        //this.value = TarimKVProto.KeyValue.newBuilder(value).build();
    }
    // Key固定编码：{chunkID}{separator}{primaryKey}{separator}{encodeVersion}
    public static String KeyEncode(KeyValueCodec kv) {
        String internalKey = new String();
        internalKey.format("%d%s%s%s%d"
                           ,kv.chunkID
                           ,KeyValueCodec.KEY_SEPARATOR
                           ,kv.value.getKey()
                           ,KeyValueCodec.KEY_SEPARATOR
                           ,kv.value.getEncodeVersion());
        return internalKey;
    }
    public static KeyValueCodec KeyDecode(String internalKey) throws TarimKVException{
        String[] result = internalKey.split(KeyValueCodec.KEY_SEPARATOR);
        if(result.length != 3) throw new TarimKVException(Status.KEY_ENCODE_ERROR);
        KeyValueCodec kvc = new KeyValueCodec();
        kvc.chunkID = Integer.parseInt(result[0]);
        TarimKVProto.KeyValue.Builder kvBuilder = TarimKVProto.KeyValue.newBuilder();
        kvBuilder.setKey(result[1]);
        kvBuilder.setEncodeVersion(Integer.parseInt(result[2]));
        kvc.value = kvBuilder.build();
        return kvc;
    }
    public static byte[] ValueEncode() {
        // un-used
        return null;
    }
    public static byte[] ValueDecode() {
        // un-used
        return null;
    }
}


