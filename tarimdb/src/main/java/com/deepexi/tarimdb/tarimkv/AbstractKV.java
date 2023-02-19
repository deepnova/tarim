package com.deepexi.tarimdb.tarimkv;

public abstract class AbstractKV {
    public int init(){
        // no-op
        return 0;
    }

    /*------ write ------*/

    public void put(KVSchema.KeyValue value) {
        // no-op
    }
    public void put(KVSchema.KVChunk chunk, KVSchema.KeyValue value) {
        // no-op
    }
    public void put(KVSchema.KVChunk chunk, List<KVSchema.KeyValue> values) {
        // no-op
    }

    /*------ point read ------*/

    public KVSchema.KeyValue get(KVSchema.KVChunk chunk) {
        // no-op
        return null;
    }

    /*------ chunk scan ------*/
    public PrepareScanInfo prepareChunkScan(String tableID, List<long> chunks){
        // no-op
        // Note: need keeping the snapshot before complete scan (snapshot counter?).
        return null;
    }

    // ifComplete is output parameter, scan not complete until ifComplete == true.
    public List<KeyValue> deltaChunkScan(DeltaScanParam param, boolean ifComplete){ 
        // no-op
        return null;
    }

    // stop scan even ifComplete == false
    public void stopChunkScan(int snapshotID){
        // no-op
        return 0;
    }

    /*------ prefix scan ------*/
    // only support prefix scan in chunk of table

    // return snapshotID 
    public int preparePrefixChunkScan(String tableID, List<long> chunks){
        // no-op
        // Note: need keeping the snapshot before complete scan (snapshot counter?).
        return null;
    }

    public List<KeyValue> prefixChunkScan(PrefixScanParam param, boolean ifComplete){ 
        // no-op
        return null;
    }

    // stop scan same as: public void stopChunkScan(int snapshotID);

}

