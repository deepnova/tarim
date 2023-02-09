package com.deepexi.tarimkv;

public abstract class AbstractKV {
    public int init(){
        // no-op
        return 0;
    }
    public int Put(KVSchema.KVTable table, KVSchema.KVChunk chunk, KVSchema.KeyValue kv) {
        // no-op
        return 0;
    }
    public int Get(KVSchema.KVTable table, KVSchema.KVChunk chunk, KVSchema.KeyValue kv) { //kv为传出参数
        // no-op
        return 0;
    }
}

